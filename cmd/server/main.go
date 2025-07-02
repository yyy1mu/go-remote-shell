package main

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/url" // NEW: 确保导入了 net/url 包
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"go-remote-shell/internal/protocol"
)

const (
	connectionTimeout = 60 * time.Second
)

type ClientManager struct {
	agents      map[string]*websocket.Conn
	sessions    map[string]*websocket.Conn
	uiToSession map[*websocket.Conn]string
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
		go cm.readFromAgent(clientId, ws)
	} else if connType == "ui" {
		log.Println("UI 已连接")
		// 调用 handleUIConnection 时，直接传递 query (url.Values 类型)
		cm.handleUIConnection(ws, query)
	} else {
		ws.Close()
	}
}

func (cm *ClientManager) registerAgent(clientId string, ws *websocket.Conn) {
	cm.Lock()
	defer cm.Unlock()
	if oldAgent, ok := cm.agents[clientId]; ok {
		oldAgent.Close()
	}
	cm.agents[clientId] = ws
}

// ==================== 关键修复：修改函数签名 ====================
// 将 query 的类型从 map[string][]string 改为 url.Values
func (cm *ClientManager) handleUIConnection(uiConn *websocket.Conn, query url.Values) {
	// 现在可以安全、方便地使用 .Get() 方法
	clientId := query.Get("clientId")
	user := query.Get("user")

	if clientId == "" {
		uiConn.WriteMessage(websocket.TextMessage, []byte("错误：缺少 'clientId' 参数。"))
		uiConn.Close()
		return
	}
	// ==========================================================

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

	cm.Lock()
	cm.sessions[sessionID] = uiConn
	cm.uiToSession[uiConn] = sessionID
	cm.Unlock()

	startMsg := protocol.Message{Type: "start_session", SessionID: sessionID, User: user}
	startMsgBytes, _ := json.Marshal(startMsg)
	if err := agentConn.WriteMessage(websocket.TextMessage, startMsgBytes); err != nil {
		log.Printf("向 Agent 发送启动指令失败: %v", err)
		cm.cleanupUISession(uiConn, agentConn)
		return
	}

	go cm.forwardFromUIToAgent(uiConn, agentConn)
}

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
			rawData, err := msg.DecodePayload()
			if err == nil {
				if err := uiConn.WriteMessage(websocket.BinaryMessage, rawData); err != nil {
					cm.cleanupUISession(uiConn, agentConn)
				}
			}
		}
	}
}

func (cm *ClientManager) forwardFromUIToAgent(uiConn *websocket.Conn, agentConn *websocket.Conn) {
	defer cm.cleanupUISession(uiConn, agentConn)

	for {
		uiConn.SetReadDeadline(time.Now().Add(connectionTimeout))
		msgType, msgData, err := uiConn.ReadMessage()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				log.Printf("UI 连接超时 (SessionID: %s)，正在关闭会话...", cm.getSessionID(uiConn))
			}
			break
		}

		var outMsg protocol.Message
		if msgType == websocket.BinaryMessage || msgType == websocket.TextMessage {
			var resizeCmd protocol.Message
			if json.Unmarshal(msgData, &resizeCmd) == nil && resizeCmd.Cols > 0 {
				outMsg = protocol.Message{Type: "resize", SessionID: cm.getSessionID(uiConn), Cols: resizeCmd.Cols, Rows: resizeCmd.Rows}
			} else {
				outMsg = protocol.NewDataMessage(cm.getSessionID(uiConn), msgData)
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
}

func (cm *ClientManager) cleanupUISession(uiConn *websocket.Conn, agentConn *websocket.Conn) {
	cm.Lock()
	defer cm.Unlock()

	if sessionID, ok := cm.uiToSession[uiConn]; ok {
		log.Printf("正在清理 UI 会话: SessionID=%s", sessionID)

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

func (cm *ClientManager) getSessionID(uiConn *websocket.Conn) string {
	cm.RLock()
	defer cm.RUnlock()
	return cm.uiToSession[uiConn]
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
