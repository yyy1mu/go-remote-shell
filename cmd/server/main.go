package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"go-remote-shell/internal/protocol"
)

const (
	// 定义UI连接的超时时间
	connectionTimeout = 60 * time.Second
	// 定义服务器端保存会话日志的目录
	logDir = "session_logs"
)

// ClientManager 统一管理所有的 Agent 连接和 UI 会话
type ClientManager struct {
	agents      map[string]*websocket.Conn // key: clientId
	sessions    map[string]*websocket.Conn // key: sessionId, value: uiConn
	uiToSession map[*websocket.Conn]string // key: uiConn, value: sessionId
	logFiles    map[string]*os.File        // key: sessionId, value: 日志文件句柄
	sync.RWMutex
}

// NewClientManager 创建并初始化一个新的 ClientManager
func NewClientManager() *ClientManager {
	// 确保日志目录存在
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Fatalf("无法创建日志目录 '%s': %v", logDir, err)
	}
	log.Printf("会话日志将保存在: %s", logDir)

	return &ClientManager{
		agents:      make(map[string]*websocket.Conn),
		sessions:    make(map[string]*websocket.Conn),
		uiToSession: make(map[*websocket.Conn]string),
		logFiles:    make(map[string]*os.File),
	}
}

// upgrader 将 HTTP 请求升级为 WebSocket 连接
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// handleConnections 是处理所有 WebSocket 连接的入口
func (cm *ClientManager) handleConnections(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	connType := query.Get("type")
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("升级 WebSocket 失败: %v", err)
		return
	}

	switch connType {
	case "agent":
		clientId := query.Get("clientId")
		if clientId == "" {
			ws.Close()
			return
		}
		log.Printf("Agent 已连接: %s", clientId)
		cm.registerAgent(clientId, ws)
		go cm.readFromAgent(clientId, ws) // 为 Agent 启动一个专用的消息读取和分发器
	case "ui":
		log.Println("UI 已连接")
		cm.handleUIConnection(ws, query) // 交给 UI 连接处理器
	default:
		log.Printf("收到未知的连接类型: %s", connType)
		ws.Close()
	}
}

// registerAgent 注册或更新一个 Agent 的连接
func (cm *ClientManager) registerAgent(clientId string, ws *websocket.Conn) {
	cm.Lock()
	defer cm.Unlock()
	if oldAgent, ok := cm.agents[clientId]; ok {
		oldAgent.Close()
	}
	cm.agents[clientId] = ws
}

// handleUIConnection 为新的 UI 连接创建独立的会话
func (cm *ClientManager) handleUIConnection(uiConn *websocket.Conn, query url.Values) {
	clientId := query.Get("clientId")
	user := query.Get("user")

	if clientId == "" {
		uiConn.WriteMessage(websocket.TextMessage, []byte("错误：缺少 'clientId' 参数。"))
		uiConn.Close()
		return
	}

	cm.RLock()
	agentConn, ok := cm.agents[clientId]
	cm.RUnlock()

	if !ok {
		uiConn.WriteMessage(websocket.TextMessage, []byte("错误：Agent '"+clientId+"' 未连接。"))
		uiConn.Close()
		return
	}

	sessionID := uuid.New().String()
	log.Printf("为 UI 创建新会话: SessionID=%s, User=%s", sessionID, user)

	// 创建日志文件
	fileName := fmt.Sprintf("session-%s-%d.log", sessionID, time.Now().Unix())
	filePath := filepath.Join(logDir, fileName)
	logFile, err := os.Create(filePath)
	if err != nil {
		log.Printf("无法创建会话日志文件 '%s': %v", filePath, err)
		uiConn.Close()
		return
	}
	log.Printf("会话 '%s' 的日志文件已创建: %s", sessionID, filePath)
	logHeader := fmt.Sprintf("--- Session Start ---\nTime: %s\nClientID: %s\nUser: %s\nSessionID: %s\n---------------------\n\n",
		time.Now().Format(time.RFC3339), clientId, user, sessionID)
	logFile.WriteString(logHeader)

	// 注册会话
	cm.Lock()
	cm.sessions[sessionID] = uiConn
	cm.uiToSession[uiConn] = sessionID
	cm.logFiles[sessionID] = logFile
	cm.Unlock()

	// 向 Agent 发送启动指令
	startMsg := protocol.Message{Type: "start_session", SessionID: sessionID, User: user}
	startMsgBytes, _ := json.Marshal(startMsg)
	if err := agentConn.WriteMessage(websocket.TextMessage, startMsgBytes); err != nil {
		log.Printf("向 Agent 发送启动指令失败: %v", err)
		cm.cleanupUISession(uiConn, agentConn)
		return
	}

	// 为 UI 启动一个独立的转发器
	go cm.forwardFromUIToAgent(uiConn, agentConn)
}

// readFromAgent 是 Agent 的总读取器，负责将消息分发给正确的 UI 并记录日志
func (cm *ClientManager) readFromAgent(clientId string, agentConn *websocket.Conn) {
	defer func() {
		log.Printf("Agent '%s' 的连接已断开，正在清理相关的所有UI会话...", clientId)
		cm.Lock()
		// 当 Agent 断开时，需要找到所有与它相关的会话并清理
		// 这个逻辑比较复杂，简单起见我们先清理 agent 本身
		delete(cm.agents, clientId)
		// 理想情况下，还需要遍历 sessions，关闭所有属于此 agent 的会话
		cm.Unlock()
		agentConn.Close()
	}()

	for {
		_, msgBytes, err := agentConn.ReadMessage()
		if err != nil {
			break
		}

		var msg protocol.Message
		if err := json.Unmarshal(msgBytes, &msg); err != nil {
			continue
		}

		rawData, err := msg.DecodePayload()
		if err != nil {
			continue
		}

		cm.RLock()
		logFile, logOk := cm.logFiles[msg.SessionID]
		uiConn, sessionOk := cm.sessions[msg.SessionID]
		cm.RUnlock()

		// 这是唯一的会话内容日志写入点
		if logOk {
			logFile.Write(rawData)
		}

		if sessionOk {
			if err := uiConn.WriteMessage(websocket.BinaryMessage, rawData); err != nil {
				// 如果写入 UI 失败，也清理这个 UI 会话
				cm.cleanupUISession(uiConn, agentConn)
			}
		}
	}
}

// forwardFromUIToAgent 将单个 UI 的消息路由给 Agent，并处理超时
func (cm *ClientManager) forwardFromUIToAgent(uiConn *websocket.Conn, agentConn *websocket.Conn) {
	defer cm.cleanupUISession(uiConn, agentConn)

	sessionID := cm.getSessionID(uiConn)

	for {
		// 设置读取超时
		uiConn.SetReadDeadline(time.Now().Add(connectionTimeout))
		_, msgData, err := uiConn.ReadMessage()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				log.Printf("UI 连接超时 (SessionID: %s)，正在关闭会话...", sessionID)
			}
			break
		}

		// 此处已不再记录用户的输入流，以解决“双写”问题

		var outMsg protocol.Message
		var resizeCmd protocol.Message
		if json.Unmarshal(msgData, &resizeCmd) == nil && resizeCmd.Cols > 0 {
			outMsg = protocol.Message{Type: "resize", SessionID: sessionID, Cols: resizeCmd.Cols, Rows: resizeCmd.Rows}
		} else {
			outMsg = protocol.NewDataMessage(sessionID, msgData)
		}

		outMsgBytes, _ := json.Marshal(outMsg)
		if agentConn != nil {
			if err := agentConn.WriteMessage(websocket.TextMessage, outMsgBytes); err != nil {
				break
			}
		} else {
			break
		}
	}
}

// cleanupUISession 清理一个独立的 UI 会话
func (cm *ClientManager) cleanupUISession(uiConn *websocket.Conn, agentConn *websocket.Conn) {
	cm.Lock()
	defer cm.Unlock()

	if sessionID, ok := cm.uiToSession[uiConn]; ok {
		log.Printf("正在清理 UI 会话: SessionID=%s", sessionID)

		// 关闭并清理日志文件句柄
		if logFile, logOk := cm.logFiles[sessionID]; logOk {
			logFile.WriteString(fmt.Sprintf("\n--- Session End [%s] ---\n", time.Now().Format(time.RFC3339)))
			logFile.Close()
			delete(cm.logFiles, sessionID)
		}

		// 通知 Agent 关闭对应的 PTY 会话
		if agentConn != nil {
			closeMsg := protocol.Message{Type: "close_session", SessionID: sessionID}
			closeMsgBytes, _ := json.Marshal(closeMsg)
			if err := agentConn.WriteMessage(websocket.TextMessage, closeMsgBytes); err != nil {
				log.Printf("向 Agent 发送关闭指令失败: %v", err)
			}
		}

		delete(cm.sessions, sessionID)
	}
	delete(cm.uiToSession, uiConn)
	uiConn.Close()
}

// getSessionID 安全地获取 sessionID
func (cm *ClientManager) getSessionID(uiConn *websocket.Conn) string {
	cm.RLock()
	defer cm.RUnlock()
	return cm.uiToSession[uiConn]
}

// main 函数启动服务器
func main() {
	clientManager := NewClientManager()
	http.Handle("/", http.FileServer(http.Dir("./web")))
	http.HandleFunc("/ws", clientManager.handleConnections)
	port := "3000"
	log.Printf("服务器正在监听 http://localhost:%s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
