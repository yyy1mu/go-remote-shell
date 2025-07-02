// File: cmd/server/main.go
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/google/uuid" // go get github.com/google/uuid
	"github.com/gorilla/websocket"
	"go-remote-shell/internal/protocol" // 引入我们自己的协议包
)

// ClientManager 现在管理 agents 和 sessions
type ClientManager struct {
	agents      map[string]*websocket.Conn // key: clientId
	sessions    map[string]*websocket.Conn // key: sessionId, value: uiConn
	uiToSession map[*websocket.Conn]string // key: uiConn, value: sessionId
	sync.RWMutex
}

func NewClientManager() *ClientManager {
	return &ClientManager{
		agents:      make(map[string]*websocket.Conn),
		sessions:    make(map[string]*websocket.Conn),
		uiToSession: make(map[*websocket.Conn]string),
	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func (cm *ClientManager) handleConnections(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	connType := query.Get("type")
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("升级 WebSocket 失败: %v", err)
		return
	}

	if connType == "agent" {
		clientId := query.Get("clientId")
		if clientId == "" {
			ws.Close()
			return
		}
		log.Printf("Agent 已连接: %s", clientId)
		cm.registerAgent(clientId, ws)
		go cm.readFromAgent(clientId, ws) // 为 Agent 启动一个专用的读取器
	} else if connType == "ui" {
		log.Println("UI 已连接")
		cm.handleUIConnection(ws, query)
	} else {
		ws.Close()
	}
}

// registerAgent 注册一个新的 agent
func (cm *ClientManager) registerAgent(clientId string, ws *websocket.Conn) {
	cm.Lock()
	defer cm.Unlock()
	if oldAgent, ok := cm.agents[clientId]; ok {
		oldAgent.Close()
	}
	cm.agents[clientId] = ws
}

// handleUIConnection 为新的 UI 创建一个独立的会话
func (cm *ClientManager) handleUIConnection(uiConn *websocket.Conn, query map[string][]string) {
	clientId := query["clientId"][0]
	user := query["user"][0]

	cm.RLock()
	agentConn, ok := cm.agents[clientId]
	cm.RUnlock()

	if !ok {
		uiConn.WriteMessage(websocket.TextMessage, []byte("错误：Agent '"+clientId+"' 未连接。"))
		uiConn.Close()
		return
	}

	// 1. 创建唯一会话 ID
	sessionID := uuid.New().String()
	log.Printf("为 UI 创建新会话: SessionID=%s, User=%s", sessionID, user)

	// 2. 注册会话
	cm.Lock()
	cm.sessions[sessionID] = uiConn
	cm.uiToSession[uiConn] = sessionID
	cm.Unlock()

	// 3. 向 Agent 发送启动指令
	startMsg := protocol.Message{
		Type:      "start_session",
		SessionID: sessionID,
		User:      user,
	}
	startMsgBytes, _ := json.Marshal(startMsg)
	if err := agentConn.WriteMessage(websocket.TextMessage, startMsgBytes); err != nil {
		log.Printf("向 Agent 发送启动指令失败: %v", err)
		cm.cleanupUISession(uiConn)
		return
	}

	// 4. 为 UI 启动转发
	go cm.forwardFromUIToAgent(uiConn, agentConn)
}

// readFromAgent 是一个 agent 的总读取器，负责将消息分发给正确的 UI
func (cm *ClientManager) readFromAgent(clientId string, agentConn *websocket.Conn) {
	defer func() {
		log.Printf("Agent '%s' 的读取器已停止。", clientId)
		cm.Lock()
		delete(cm.agents, clientId)
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

		cm.RLock()
		uiConn, ok := cm.sessions[msg.SessionID]
		cm.RUnlock()

		if ok {
			// 解码 payload 并直接发送原始二进制数据给 UI
			rawData, err := msg.DecodePayload()
			if err == nil {
				if err := uiConn.WriteMessage(websocket.BinaryMessage, rawData); err != nil {
					// 如果写入UI失败，可以清理这个UI会话
					cm.cleanupUISession(uiConn)
				}
			}
		}
	}
}

// forwardFromUIToAgent 将单个 UI 的消息路由给 Agent
func (cm *ClientManager) forwardFromUIToAgent(uiConn *websocket.Conn, agentConn *websocket.Conn) {
	defer cm.cleanupUISession(uiConn)

	cm.RLock()
	sessionID := cm.uiToSession[uiConn]
	cm.RUnlock()

	for {
		msgType, msgData, err := uiConn.ReadMessage()
		if err != nil {
			break
		}

		var outMsg protocol.Message
		if msgType == websocket.BinaryMessage || msgType == websocket.TextMessage {
			// 尝试解析为 resize 命令
			var resizeCmd protocol.Message
			if json.Unmarshal(msgData, &resizeCmd) == nil && resizeCmd.Cols > 0 {
				outMsg = protocol.Message{
					Type:      "resize",
					SessionID: sessionID,
					Cols:      resizeCmd.Cols,
					Rows:      resizeCmd.Rows,
				}
			} else {
				// 否则视为普通数据
				outMsg = protocol.NewDataMessage(sessionID, msgData)
			}

			outMsgBytes, _ := json.Marshal(outMsg)
			if err := agentConn.WriteMessage(websocket.TextMessage, outMsgBytes); err != nil {
				break
			}
		}
	}
}

// cleanupUISession 清理一个 UI 会话
func (cm *ClientManager) cleanupUISession(uiConn *websocket.Conn) {
	cm.Lock()
	defer cm.Unlock()

	if sessionID, ok := cm.uiToSession[uiConn]; ok {
		log.Printf("正在清理 UI 会话: SessionID=%s", sessionID)
		delete(cm.sessions, sessionID)
	}
	delete(cm.uiToSession, uiConn)
	uiConn.Close()
}

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
