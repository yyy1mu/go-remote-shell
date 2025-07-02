// File: cmd/agent/main.go
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os/exec"
	"os/user"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/gorilla/websocket"
	"go-remote-shell/internal/protocol"
)

// SessionManager 保持不变
type SessionManager struct {
	sessions map[string]chan protocol.Message
	sync.RWMutex
}

func (sm *SessionManager) Get(id string) (chan protocol.Message, bool) {
	sm.RLock()
	defer sm.RUnlock()
	ch, ok := sm.sessions[id]
	return ch, ok
}

func (sm *SessionManager) Add(id string, ch chan protocol.Message) {
	sm.Lock()
	defer sm.Unlock()
	sm.sessions[id] = ch
}

func (sm *SessionManager) Remove(id string) {
	sm.Lock()
	defer sm.Unlock()
	if ch, ok := sm.sessions[id]; ok {
		close(ch) // 关闭 channel 会触发 pty 会话的清理
		delete(sm.sessions, id)
	}
}

var (
	serverAddr = flag.String("addr", "10.0.0.178:3000", "WebSocket server address")
	clientId   = flag.String("id", "golang-client-1", "Client ID for this agent")
)

func main() {
	flag.Parse()
	log.Printf("Agent启动，ID: %s", *clientId)

	u := url.URL{Scheme: "ws", Host: *serverAddr, Path: "/ws", RawQuery: "type=agent&clientId=" + *clientId}
	for {
		log.Printf("正在连接到 %s", u.String())
		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		if err != nil {
			log.Println("连接失败，将在5秒后重试:", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Println("连接成功，启动指令分发器...")
		runCommandDispatcher(c)

		log.Println("与服务器的连接已断开，准备重连...")
		c.Close()
	}
}

// runCommandDispatcher 增加对 close_session 的处理
func runCommandDispatcher(c *websocket.Conn) {
	sm := &SessionManager{sessions: make(map[string]chan protocol.Message)}
	writerChan := make(chan protocol.Message)

	go messageWriter(c, writerChan)

	for {
		_, msgBytes, err := c.ReadMessage()
		if err != nil {
			sm.Lock()
			for id := range sm.sessions {
				sm.Remove(id)
			}
			sm.Unlock()
			close(writerChan)
			return
		}

		var msg protocol.Message
		if err := json.Unmarshal(msgBytes, &msg); err != nil {
			continue
		}

		switch msg.Type {
		case "start_session":
			log.Printf("收到启动会话指令: SessionID=%s, User=%s", msg.SessionID, msg.User)
			inputChan := make(chan protocol.Message)
			sm.Add(msg.SessionID, inputChan)
			go handlePtySession(msg.SessionID, msg.User, inputChan, writerChan, func() {
				sm.Remove(msg.SessionID)
			})

		case "data", "resize":
			if ch, ok := sm.Get(msg.SessionID); ok {
				ch <- msg
			}

		// NEW: 处理关闭会话指令
		case "close_session":
			log.Printf("收到关闭会话指令: SessionID=%s", msg.SessionID)
			sm.Remove(msg.SessionID)
		}
	}
}

// messageWriter 和 handlePtySession 保持不变
func messageWriter(c *websocket.Conn, ch chan protocol.Message) {
	for msg := range ch {
		msgBytes, _ := json.Marshal(msg)
		if err := c.WriteMessage(websocket.TextMessage, msgBytes); err != nil {
			break
		}
	}
}

func handlePtySession(sessionID, username string, inputChan chan protocol.Message, writerChan chan protocol.Message, onExit func()) {
	defer func() {
		log.Printf("PTY 会话 '%s' 已结束。", sessionID)
		onExit()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := exec.Command("bash", "-i")
	if username != "" {
		u, err := user.Lookup(username)
		if err != nil {
			log.Printf("查找用户 '%s' 失败: %v", username, err)
			return
		}
		uid, _ := strconv.ParseUint(u.Uid, 10, 32)
		gid, _ := strconv.ParseUint(u.Gid, 10, 32)
		cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)}}
		cmd.Env = []string{"TERM=xterm", fmt.Sprintf("HOME=%s", u.HomeDir), fmt.Sprintf("USER=%s", username), "PATH=/usr/local/bin:/usr/bin:/bin"}
	}

	ptmx, err := pty.Start(cmd)
	if err != nil {
		log.Printf("启动 pty 失败: %v", err)
		return
	}
	defer ptmx.Close()

	go func() {
		buffer := make([]byte, 4096)
		for {
			n, err := ptmx.Read(buffer)
			if err != nil {
				cancel()
				return
			}
			msg := protocol.NewDataMessage(sessionID, buffer[:n])
			writerChan <- msg
		}
	}()

	go func() {
		for msg := range inputChan {
			switch msg.Type {
			case "data":
				data, err := msg.DecodePayload()
				if err == nil {
					ptmx.Write(data)
				}
			case "resize":
				pty.Setsize(ptmx, &pty.Winsize{Rows: msg.Rows, Cols: msg.Cols})
			}
		}
		cancel()
	}()

	<-ctx.Done()
	cmd.Wait()
}
