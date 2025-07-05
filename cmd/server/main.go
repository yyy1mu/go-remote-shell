// File: cmd/server/main.go
package main

import (
	"encoding/base64"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	logDir = "session_logs" // 日志目录
)

// ClientManager 现在再次管理日志文件，但以更智能的方式
type ClientManager struct {
	agents      map[string]*websocket.Conn // key: clientId
	uis         map[string]*websocket.Conn // key: clientId
	logFiles    map[string]*os.File        // key: clientId (简化模型，一个clientId只对应一个日志文件)
	sessionData map[string]string          // key: clientId, value: user (用于日志头)
	sync.RWMutex
}

func NewClientManager() *ClientManager {
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Fatalf("无法创建日志目录 '%s': %v", logDir, err)
	}
	log.Printf("会话日志将保存在: %s", logDir)
	return &ClientManager{
		agents:      make(map[string]*websocket.Conn),
		uis:         make(map[string]*websocket.Conn),
		logFiles:    make(map[string]*os.File),
		sessionData: make(map[string]string),
	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func (cm *ClientManager) handleConnections(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	connType := query.Get("type")
	clientId := query.Get("clientId")
	if connType == "" || clientId == "" {
		http.Error(w, "'type' and 'clientId' are required", http.StatusBadRequest)
		return
	}
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	if connType == "agent" {
		cm.handleAgentConnection(clientId, ws)
	} else if connType == "ui" {
		// UI 连接时，创建日志文件
		user := query.Get("user")
		cm.createLogFile(clientId, user)
		cm.handleUIConnection(clientId, ws)
	} else {
		ws.Close()
	}
}

func (cm *ClientManager) handleAgentConnection(clientId string, agentConn *websocket.Conn) {
	log.Printf("Agent 已连接: %s", clientId)
	cm.Lock()
	if old, ok := cm.agents[clientId]; ok {
		old.Close()
	}
	cm.agents[clientId] = agentConn
	cm.Unlock()

	defer func() {
		log.Printf("Agent 已断开: %s", clientId)
		cm.Lock()
		if current, ok := cm.agents[clientId]; ok && current == agentConn {
			delete(cm.agents, clientId)
		}
		cm.Unlock()
		agentConn.Close()
	}()
	forward(agentConn, "Agent -> UI", cm, clientId)
}

func (cm *ClientManager) handleUIConnection(clientId string, uiConn *websocket.Conn) {
	log.Printf("UI 已连接，请求 ClientID: %s", clientId)
	cm.Lock()
	if old, ok := cm.uis[clientId]; ok {
		old.Close()
	}
	cm.uis[clientId] = uiConn
	cm.Unlock()

	defer func() {
		log.Printf("UI 已断开，请求 ClientID: %s", clientId)
		cm.Lock()
		if current, ok := cm.uis[clientId]; ok && current == uiConn {
			delete(cm.uis, clientId)
		}
		// 关闭并删除日志文件句柄
		if logFile, ok := cm.logFiles[clientId]; ok {
			logFile.WriteString(fmt.Sprintf("\n--- Session End [%s] ---\n", time.Now().Format(time.RFC3339)))
			logFile.Close()
			delete(cm.logFiles, clientId)
		}
		delete(cm.sessionData, clientId)
		cm.Unlock()
		uiConn.Close()
	}()
	forward(uiConn, "UI -> Agent", cm, clientId)
}

func (cm *ClientManager) createLogFile(clientId, user string) {
	cm.Lock()
	defer cm.Unlock()
	// 如果已有日志文件，先关闭
	if oldLog, ok := cm.logFiles[clientId]; ok {
		oldLog.Close()
	}
	logFileName := fmt.Sprintf("session-%s-%d.log", clientId, time.Now().Unix())
	filePath := filepath.Join(logDir, logFileName)
	logFile, err := os.Create(filePath)
	if err != nil {
		log.Printf("无法为 ClientID %s 创建日志文件: %v", clientId, err)
		return
	}

	// 从 FIDO2 流程获取的 SessionID 在此不可知，但我们可以记录 User 和 ClientID
	logHeader := fmt.Sprintf("--- Session Start ---\nTime: %s\nClientID: %s\nUser: %s\n---------------------\n\n",
		time.Now().Format(time.RFC3339), clientId, user)
	logFile.WriteString(logHeader)

	cm.logFiles[clientId] = logFile
	cm.sessionData[clientId] = user
	log.Printf("为 ClientID '%s' 开始记录日志到: %s", clientId, logFileName)
}

func forward(src *websocket.Conn, direction string, cm *ClientManager, clientId string) {
	for {
		msgType, msg, err := src.ReadMessage()
		if err != nil {
			log.Printf("停止转发 (%s)，原因: 读取错误: %v", direction, err)
			return
		}

		// --- 日志记录 ---
		cm.RLock()
		logFile, logOk := cm.logFiles[clientId]
		cm.RUnlock()

		if logOk {
			// 写入结构化日志: [TIMESTAMP] [DIRECTION] BASE64_PAYLOAD
			dir := "IN" // 默认为输入
			if direction == "Agent -> UI" {
				dir = "OUT"
			}
			logLine := fmt.Sprintf("[%s] [%s] %s\n",
				time.Now().Format(time.RFC3339Nano),
				dir,
				base64.StdEncoding.EncodeToString(msg))
			logFile.WriteString(logLine)
		}
		// ---

		// 动态获取最新的 dest 连接
		var dest *websocket.Conn
		cm.RLock()
		if direction == "UI -> Agent" {
			dest = cm.agents[clientId]
		} else { // Agent -> UI
			dest = cm.uis[clientId]
		}
		cm.RUnlock()

		if dest != nil {
			if err = dest.WriteMessage(msgType, msg); err != nil {
				log.Printf("停止转发 (%s)，原因: 写入错误: %v", direction, err)
				return
			}
		}
	}
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
