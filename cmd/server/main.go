package main

import (
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

// connectionPair 用于存储配对的 agent 和 ui 的 WebSocket 连接
type connectionPair struct {
	agent *websocket.Conn
	ui    *websocket.Conn
}

// ClientManager 是一个线程安全的结构，用于管理所有客户端连接
type ClientManager struct {
	clients map[string]*connectionPair
	sync.Mutex
}

// NewClientManager 创建一个新的 ClientManager
func NewClientManager() *ClientManager {
	return &ClientManager{
		clients: make(map[string]*connectionPair),
	}
}

// upgrader 用于将 HTTP 连接升级为 WebSocket 连接
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		// 允许所有来源的连接，用于开发；生产环境中应更严格
		return true
	},
}

// handleConnections 是处理 WebSocket 连接请求的主要函数
func (cm *ClientManager) handleConnections(w http.ResponseWriter, r *http.Request) {
	// 从 URL query 中获取参数
	query := r.URL.Query()
	clientId := query.Get("clientId")
	connType := query.Get("type")

	if clientId == "" || connType == "" {
		log.Println("连接被拒绝: clientId 和 type 为必填项。")
		http.Error(w, "clientId and type are required", http.StatusBadRequest)
		return
	}

	// 升级 HTTP 连接为 WebSocket
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("升级 WebSocket 失败: %v", err)
		return
	}
	// defer ws.Close() // defer a ws.Close() here will close the connection immediately after handleConnections returns.
	// The connection should be kept open for the forwarders. Closing is handled by the forwarder's defer.

	// --- 连接管理 ---
	cm.Lock()

	// 如果 clientId 不存在，则初始化
	if _, ok := cm.clients[clientId]; !ok {
		cm.clients[clientId] = &connectionPair{}
	}
	pair := cm.clients[clientId]

	if connType == "agent" {
		log.Printf("Agent 已连接: %s", clientId)
		if pair.agent != nil {
			pair.agent.Close() // Close any existing agent connection for this ID
		}
		pair.agent = ws
	} else if connType == "ui" {
		log.Printf("UI 已连接: %s", clientId)
		if pair.ui != nil {
			pair.ui.Close() // Close any existing ui connection for this ID
		}
		pair.ui = ws
	} else {
		log.Printf("未知的连接类型: %s", connType)
		cm.Unlock()
		ws.Close()
		return
	}

	// 检查是否配对成功，如果成功则启动双向转发
	agentConn, uiConn := pair.agent, pair.ui
	if agentConn != nil && uiConn != nil {
		log.Printf("为 %s 配对成功。开始转发数据。", clientId)
		// 启动两个 goroutine 分别处理双向的数据流
		// We need to release the lock before starting the goroutines to avoid deadlock.
		cm.Unlock()
		go forward(uiConn, agentConn, "UI -> Agent", clientId, cm) // UI 到 Agent
		go forward(agentConn, uiConn, "Agent -> UI", clientId, cm) // Agent 到 UI
	} else {
		cm.Unlock() // 释放锁
	}

	// The select{} block from original code is not needed here.
	// The handleConnections function will return, but the established websocket connection
	// and the forward goroutines will remain active.
}

// forward 函数在两个 WebSocket 连接之间转发讯息
func forward(src, dest *websocket.Conn, direction string, clientId string, cm *ClientManager) {
	// 当此函数结束时（通常是来源连接断开），清理连接
	defer func() {
		log.Printf("转发停止 (%s) for %s", direction, clientId)
		cm.Lock()
		defer cm.Unlock()

		pair, ok := cm.clients[clientId]
		if !ok {
			return // Pair might have been already cleaned up
		}

		// 根据方向，通知另一端连接已断开
		if direction == "Agent -> UI" {
			if pair.ui != nil {
				pair.ui.WriteMessage(websocket.TextMessage, []byte("\r\n--- AGENT DISCONNECTED ---\r\n"))
			}
			pair.agent = nil
		} else { // UI -> Agent
			if pair.agent != nil {
				// We could notify the agent, but it's often not necessary.
			}
			pair.ui = nil
		}

		// 如果两者都断开，可以从 map 中移除
		if pair.agent == nil && pair.ui == nil {
			delete(cm.clients, clientId)
			log.Printf("已为 %s 清理所有连接。", clientId)
		}

		src.Close() // 确保来源连接被关闭
	}()

	for {
		// 从来源读取讯息
		msgType, msg, err := src.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("读取错误 (%s): %v", direction, err)
			} else {
				log.Printf("连接正常关闭 (%s)", direction)
			}
			break
		}

		// 将讯息写入目的地
		if dest != nil {
			if err = dest.WriteMessage(msgType, msg); err != nil {
				log.Printf("写入错误 (%s): %v", direction, err)
				break
			}
		}
	}
}

func main() {
	clientManager := NewClientManager()

	// 1. **修改点**: 设置静态文件服务器，用于提供 web/ 目录下的文件
	http.Handle("/", http.FileServer(http.Dir("./web")))

	// 2. 设置 WebSocket 处理函数
	http.HandleFunc("/ws", clientManager.handleConnections)

	port := "3000"
	log.Printf("服务器正在监听 http://localhost:%s", port)
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
