package main

import (
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

// agentSession 用于存储一个 Agent 和所有连接到它的 UI
type agentSession struct {
	agent        *websocket.Conn
	uis          map[*websocket.Conn]bool
	sync.RWMutex // 用于保护 uis 集合的读写
}

// ClientManager 管理所有 agentSession
type ClientManager struct {
	clients map[string]*agentSession
	sync.Mutex
}

func NewClientManager() *ClientManager {
	return &ClientManager{
		clients: make(map[string]*agentSession),
	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func (cm *ClientManager) handleConnections(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	clientId := query.Get("clientId")
	connType := query.Get("type")

	if clientId == "" || connType == "" {
		http.Error(w, "clientId and type are required", http.StatusBadRequest)
		return
	}

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("升级 WebSocket 失败: %v", err)
		return
	}

	cm.Lock()
	session, ok := cm.clients[clientId]
	if !ok {
		// 如果会话不存在，则创建一个新的
		session = &agentSession{
			uis: make(map[*websocket.Conn]bool),
		}
		cm.clients[clientId] = session
	}
	cm.Unlock()

	if connType == "agent" {
		log.Printf("Agent 已连接: %s", clientId)
		// 如果已有 Agent，先断开旧的
		if session.agent != nil {
			session.agent.Close()
		}
		session.agent = ws
		// Agent 连接后，启动一个协程，负责将从 Agent 收到的消息广播给所有 UI
		go cm.broadcastAgentToUIs(session)

	} else if connType == "ui" {
		log.Printf("UI 已连接: %s", clientId)
		cm.handleUIConnection(session, ws)
	} else {
		log.Printf("未知的连接类型: %s", connType)
		ws.Close()
	}
}

// handleUIConnection 处理新的 UI 连接
func (cm *ClientManager) handleUIConnection(session *agentSession, uiConn *websocket.Conn) {
	session.Lock()
	// 检查 Agent 是否存在
	if session.agent == nil {
		log.Println("UI 连接失败：Agent 尚未连接。")
		session.Unlock()
		uiConn.WriteMessage(websocket.TextMessage, []byte("错误：Agent 尚未连接。"))
		uiConn.Close()
		return
	}

	// 如果这是第一个连接的 UI，则向 Agent 发送启动指令
	if len(session.uis) == 0 {
		log.Printf("第一个 UI 已连接，向 Agent 发送 start_session 指令")
		startMessage := []byte(`{"type":"start_session"}`)
		if err := session.agent.WriteMessage(websocket.TextMessage, startMessage); err != nil {
			log.Printf("向 Agent 发送启动指令失败: %v", err)
			session.Unlock()
			uiConn.Close()
			return
		}
	}

	// 将新 UI 添加到会话的 UI 集合中
	session.uis[uiConn] = true
	session.Unlock()

	// 为这个 UI 启动一个独立的协程，负责将从它收到的消息转发给 Agent
	cm.forwardUIToAgent(session, uiConn)
}

// broadcastAgentToUIs 从 Agent 读取消息并广播给所有 UI
func (cm *ClientManager) broadcastAgentToUIs(session *agentSession) {
	defer func() {
		// Agent 断开，关闭所有 UI 连接并清理会话
		log.Println("Agent 连接已断开，正在关闭所有 UI 连接...")
		session.Lock()
		if session.agent != nil {
			session.agent.Close()
			session.agent = nil
		}
		for ui := range session.uis {
			ui.Close()
		}
		cm.Lock()
		delete(cm.clients, session.getClientId()) // 假设有方法获取clientId
		cm.Unlock()
		session.Unlock()
	}()

	for {
		msgType, msg, err := session.agent.ReadMessage()
		if err != nil {
			log.Printf("从 Agent 读取消息失败: %v", err)
			break
		}

		session.RLock()
		// 遍历所有 UI，将消息发送给它们
		for ui := range session.uis {
			if err := ui.WriteMessage(msgType, msg); err != nil {
				log.Printf("向 UI 写入消息失败: %v", err)
				// 可选择在此处处理发送失败的 UI（例如，移除它）
			}
		}
		session.RUnlock()
	}
}

// forwardUIToAgent 从单个 UI 读取消息并转发给 Agent
func (cm *ClientManager) forwardUIToAgent(session *agentSession, uiConn *websocket.Conn) {
	defer func() {
		// UI 断开，只需将自己从会话中移除
		log.Println("一个 UI 连接已断开。")
		session.Lock()
		delete(session.uis, uiConn)
		session.Unlock()
		uiConn.Close()
	}()

	for {
		// 检查 Agent 是否还在线
		session.RLock()
		agentExists := session.agent != nil
		session.RUnlock()
		if !agentExists {
			break
		}

		msgType, msg, err := uiConn.ReadMessage()
		if err != nil {
			if !websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("UI 连接正常关闭。")
			} else {
				log.Printf("从 UI 读取消息失败: %v", err)
			}
			break
		}

		// 将消息写入 Agent
		if err := session.agent.WriteMessage(msgType, msg); err != nil {
			log.Printf("向 Agent 写入消息失败: %v", err)
			break
		}
	}
}

// 辅助方法，用于在日志中找到 clientId (实际生产中应有更好的方式)
func (s *agentSession) getClientId() string {
	if s.agent != nil {
		return s.agent.RemoteAddr().String() // 仅为示例
	}
	return "unknown"
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
