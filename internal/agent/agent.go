package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/go-webauthn/webauthn/protocol"
	"github.com/go-webauthn/webauthn/webauthn"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	internal_protocol "go-remote-shell/internal/protocol"
)

// --- 用户和凭据存储 ---

// User 结构体实现了 webauthn.User 接口
type User struct {
	ID          []byte                `json:"id"`
	Name        string                `json:"name"`
	DisplayName string                `json:"displayName"`
	Credentials []webauthn.Credential `json:"credentials"`
}

// WebAuthnID a unique identifier for the user.
func (u *User) WebAuthnID() []byte { return u.ID }

// WebAuthnName the user's username.
func (u *User) WebAuthnName() string { return u.Name }

// WebAuthnDisplayName the user's display name.
func (u *User) WebAuthnDisplayName() string { return u.DisplayName }

// WebAuthnIcon a URL to an icon for the user.
func (u *User) WebAuthnIcon() string { return "" }

// WebAuthnCredentials the user's credentials.
func (u *User) WebAuthnCredentials() []webauthn.Credential { return u.Credentials }

// UserDB 用于在本地 JSON 文件中管理用户数据
type UserDB struct {
	path  string
	users map[string]*User // key: username
	sync.RWMutex
}

func NewUserDB(path string) (*UserDB, error) {
	db := &UserDB{path: path, users: make(map[string]*User)}
	if err := db.Load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return db, nil
}

func (db *UserDB) Load() error {
	db.Lock()
	defer db.Unlock()
	data, err := os.ReadFile(db.path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &db.users)
}

func (db *UserDB) Save() error {
	db.RLock()
	defer db.RUnlock()
	data, err := json.MarshalIndent(db.users, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(db.path, data, 0644)
}

func (db *UserDB) GetUser(name string) (*User, bool) {
	db.RLock()
	defer db.RUnlock()
	user, ok := db.users[name]
	return user, ok
}

func (db *UserDB) PutUser(user *User) {
	db.Lock()
	defer db.Unlock()
	db.users[user.Name] = user
}

// --- Agent 主结构体和方法 ---

// PtySession 封装了一个独立的 PTY 会话
type PtySession struct {
	inputChan chan internal_protocol.Message
	cancel    context.CancelFunc
}

type Agent struct {
	clientID       string
	serverURL      string
	webAuthn       *webauthn.WebAuthn
	userDB         *UserDB
	sessionData    map[string]*webauthn.SessionData // 手动实现的、用于FIDO2握手的会话存储
	activeSessions map[string]*PtySession           // 运行中的PTY会话
	sessionMutex   sync.RWMutex
	writerChan     chan internal_protocol.Message
	conn           *websocket.Conn
}

func New(clientID, serverURL, dbPath, rpID, rpOrigin string) (*Agent, error) {
	wconfig := &webauthn.Config{
		RPID:          rpID,
		RPDisplayName: "Go Remote Shell Agent",
		RPOrigins:     []string{rpOrigin},
	}
	var err error
	webAuthn, err := webauthn.New(wconfig)
	if err != nil {
		return nil, fmt.Errorf("无法创建 webauthn 实例: %v", err)
	}

	userDB, err := NewUserDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("无法加载用户数据库: %v", err)
	}

	return &Agent{
		clientID:       clientID,
		serverURL:      serverURL,
		webAuthn:       webAuthn,
		userDB:         userDB,
		sessionData:    make(map[string]*webauthn.SessionData),
		activeSessions: make(map[string]*PtySession),
	}, nil
}

func (a *Agent) Run() {
	u := url.URL{Scheme: "ws", Host: a.serverURL, Path: "/ws", RawQuery: "type=agent&clientId=" + a.clientID}
	for {
		log.Printf("正在连接到 %s", u.String())
		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		if err != nil {
			log.Printf("连接失败: %v, 5秒后重试...", err)
			time.Sleep(5 * time.Second)
			continue
		}
		a.conn = c
		a.writerChan = make(chan internal_protocol.Message, 100) // 增加缓冲区

		go a.messageWriter()
		a.listenForCommands() // 这个函数会阻塞直到连接断开

		a.conn.Close()
		log.Println("与服务器的连接已断开，准备重连...")
	}
}

func (a *Agent) listenForCommands() {
	log.Println("连接成功，开始监听指令...")
	defer func() {
		a.sessionMutex.Lock()
		for _, session := range a.activeSessions {
			session.cancel()
		}
		a.activeSessions = make(map[string]*PtySession)
		a.sessionMutex.Unlock()
		if a.writerChan != nil {
			close(a.writerChan)
		}
	}()

	for {
		var msg internal_protocol.Message
		err := a.conn.ReadJSON(&msg)
		if err != nil {
			log.Printf("读取消息失败: %v", err)
			return
		}

		// 将耗时操作放入协程，防止阻塞主读取循环
		switch msg.Type {
		case "webauthn_begin_register":
			go a.handleBeginRegister(msg)
		case "webauthn_finish_register":
			go a.handleFinishRegister(msg)
		case "webauthn_begin_login":
			go a.handleBeginLogin(msg)
		case "webauthn_finish_login":
			go a.handleFinishLogin(msg)
		case "data", "resize":
			a.sessionMutex.RLock()
			session, ok := a.activeSessions[msg.SessionID]
			a.sessionMutex.RUnlock()
			if ok {
				session.inputChan <- msg // 将数据路由到对应会话的 channel
			}
		case "close_session":
			go a.handleCloseSession(msg)
		}
	}
}

func (a *Agent) messageWriter() {
	for msg := range a.writerChan {
		if a.conn != nil {
			if err := a.conn.WriteJSON(msg); err != nil {
				log.Printf("写入 WebSocket 失败: %v", err)
				break
			}
		}
	}
}

func (a *Agent) handleBeginRegister(msg internal_protocol.Message) {
	username := msg.User
	if username == "" {
		a.writerChan <- internal_protocol.Message{Type: "error", User: username, Error: "注册需要提供用户名"}
		return
	}

	user, ok := a.userDB.GetUser(username)
	if !ok {
		log.Printf("用户 '%s' 不存在，为其创建新记录", username)
		user = &User{ID: []byte(uuid.NewString()), Name: username, DisplayName: username}
		a.userDB.PutUser(user)
	}

	options, session, err := a.webAuthn.BeginRegistration(user)
	if err != nil {
		a.writerChan <- internal_protocol.Message{Type: "error", User: username, Error: "无法开始注册: " + err.Error()}
		return
	}

	a.sessionMutex.Lock()
	a.sessionData[username] = session
	a.sessionMutex.Unlock()
	optionsJson, _ := json.Marshal(options)
	a.writerChan <- internal_protocol.Message{Type: "webauthn_register_options", User: username, Payload: string(optionsJson)}
}

func (a *Agent) handleFinishRegister(msg internal_protocol.Message) {
	username := msg.User
	user, ok := a.userDB.GetUser(username)
	if !ok {
		a.writerChan <- internal_protocol.Message{Type: "error", User: username, Error: "用户不存在"}
		return
	}

	a.sessionMutex.RLock()
	session := a.sessionData[username]
	a.sessionMutex.RUnlock()
	if session == nil {
		a.writerChan <- internal_protocol.Message{Type: "error", User: username, Error: "注册会话已过期"}
		return
	}

	payloadReader := strings.NewReader(msg.Payload)
	parsedResponse, err := protocol.ParseCredentialCreationResponseBody(payloadReader)
	if err != nil {
		a.writerChan <- internal_protocol.Message{Type: "error", User: username, Error: "无法解析注册响应: " + err.Error()}
		return
	}

	credential, err := a.webAuthn.CreateCredential(user, *session, parsedResponse)
	if err != nil {
		a.writerChan <- internal_protocol.Message{Type: "error", User: username, Error: "无法创建凭据: " + err.Error()}
		return
	}

	user.Credentials = append(user.Credentials, *credential)
	if err := a.userDB.Save(); err != nil {
		a.writerChan <- internal_protocol.Message{Type: "error", User: username, Error: "无法保存凭据"}
		return
	}

	a.sessionMutex.Lock()
	delete(a.sessionData, username)
	a.sessionMutex.Unlock()
	a.writerChan <- internal_protocol.Message{Type: "register_success", User: username}
	log.Printf("用户 '%s' 成功注册了一个新密钥。", username)
}

func (a *Agent) handleBeginLogin(msg internal_protocol.Message) {
	username := msg.User
	user, ok := a.userDB.GetUser(username)
	if !ok {
		a.writerChan <- internal_protocol.Message{Type: "error", User: username, Error: "用户不存在或未注册"}
		return
	}

	options, session, err := a.webAuthn.BeginLogin(user)
	if err != nil {
		a.writerChan <- internal_protocol.Message{Type: "error", User: username, Error: "无法开始登录: " + err.Error()}
		return
	}

	a.sessionMutex.Lock()
	a.sessionData[username] = session
	a.sessionMutex.Unlock()
	optionsJson, _ := json.Marshal(options)
	a.writerChan <- internal_protocol.Message{Type: "webauthn_login_options", User: username, Payload: string(optionsJson)}
}

func (a *Agent) handleFinishLogin(msg internal_protocol.Message) {
	username := msg.User
	user, ok := a.userDB.GetUser(username)
	if !ok {
		a.writerChan <- internal_protocol.Message{Type: "error", User: username, Error: "用户不存在"}
		return
	}

	a.sessionMutex.RLock()
	session := a.sessionData[username]
	a.sessionMutex.RUnlock()
	if session == nil {
		a.writerChan <- internal_protocol.Message{Type: "error", User: username, Error: "登录会话已过期"}
		return
	}

	payloadReader := strings.NewReader(msg.Payload)
	parsedResponse, err := protocol.ParseCredentialRequestResponseBody(payloadReader)
	if err != nil {
		a.writerChan <- internal_protocol.Message{Type: "error", User: username, Error: "无法解析登录响应: " + err.Error()}
		return
	}

	_, err = a.webAuthn.ValidateLogin(user, *session, parsedResponse)
	if err != nil {
		a.writerChan <- internal_protocol.Message{Type: "error", User: username, Error: "登录验证失败: " + err.Error()}
		return
	}

	a.sessionMutex.Lock()
	delete(a.sessionData, username)
	a.sessionMutex.Unlock()

	log.Printf("用户 '%s' FIDO2 登录成功！", user.Name)
	sessionID := uuid.New().String()

	a.writerChan <- internal_protocol.Message{Type: "login_success", SessionID: sessionID, User: username}
	go a.handlePtySession(username, sessionID)
}

func (a *Agent) handleCloseSession(msg internal_protocol.Message) {
	a.sessionMutex.Lock()
	if session, ok := a.activeSessions[msg.SessionID]; ok {
		log.Printf("收到关闭会话指令: SessionID=%s", msg.SessionID)
		session.cancel()
		delete(a.activeSessions, msg.SessionID)
	}
	a.sessionMutex.Unlock()
}

func (a *Agent) handlePtySession(username, sessionID string) {
	ctx, cancel := context.WithCancel(context.Background())

	session := &PtySession{
		inputChan: make(chan internal_protocol.Message, 10),
		cancel:    cancel,
	}
	a.sessionMutex.Lock()
	a.activeSessions[sessionID] = session
	a.sessionMutex.Unlock()

	defer func() {
		log.Printf("PTY 会话 '%s' 已结束。", sessionID)
		a.sessionMutex.Lock()
		delete(a.activeSessions, sessionID)
		a.sessionMutex.Unlock()
		cancel()
	}()

	cmd := exec.Command("bash", "-i")
	if username != "" && os.Getuid() == 0 {
		u, err := user.Lookup(username)
		if err == nil {
			uid, _ := strconv.ParseUint(u.Uid, 10, 32)
			gid, _ := strconv.ParseUint(u.Gid, 10, 32)
			cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)}}
			cmd.Env = []string{"TERM=xterm", fmt.Sprintf("HOME=%s", u.HomeDir), fmt.Sprintf("USER=%s", username), "PATH=/usr/local/bin:/usr/bin:/bin"}
		}
	}

	ptmx, err := pty.Start(cmd)
	if err != nil {
		log.Printf("启动 PTY 失败 for session %s: %v", sessionID, err)
		return
	}
	defer ptmx.Close()

	// PTY -> Server
	go func() {
		buffer := make([]byte, 4096)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				n, err := ptmx.Read(buffer)
				if err != nil {
					if err != io.EOF {
						log.Printf("PTY 读取错误 for session %s: %v", sessionID, err)
					}
					cancel()
					return
				}
				if n > 0 {
					a.writerChan <- internal_protocol.NewDataMessage(sessionID, buffer[:n])
				}
			}
		}
	}()

	log.Printf("PTY 会话 '%s' 已为用户 '%s' 启动，等待输入...", sessionID, username)

	// Server -> PTY
	for {
		select {
		case msg := <-session.inputChan:
			switch msg.Type {
			case "data":
				data, _ := msg.DecodePayload()
				ptmx.Write(data)
			case "resize":
				pty.Setsize(ptmx, &pty.Winsize{Rows: msg.Rows, Cols: msg.Cols})
			}
		case <-ctx.Done():
			cmd.Wait()
			return
		}
	}
}
