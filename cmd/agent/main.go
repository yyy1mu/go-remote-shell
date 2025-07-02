package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/url"
	"os"
	"os/exec"
	"sync/atomic" // NEW: 引入原子操作库
	"time"

	"github.com/creack/pty"
	"github.com/gorilla/websocket"
)

// ... 数据结构定义保持不变 ...
type ResizeMessage struct {
	Cols uint16 `json:"cols"`
	Rows uint16 `json:"rows"`
}

type ControlMessage struct {
	Type string `json:"type"`
}

var (
	serverAddr = flag.String("addr", "10.0.0.178:3000", "WebSocket server address")
	clientId   = flag.String("id", "golang-client-1", "Client ID for this agent")
	// NEW: 用于标记当前是否正在一个 PTY 会话中 (0: false, 1: true)
	inSession atomic.Bool
)

// main 函数保持不变
func main() {
	flag.Parse()
	log.Printf("Agent启动，ID: %s, 准备连接到服务器: %s", *clientId, *serverAddr)
	u := url.URL{Scheme: "ws", Host: *serverAddr, Path: "/ws", RawQuery: "type=agent&clientId=" + *clientId}
	for {
		log.Printf("正在连接到 %s", u.String())
		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		if err != nil {
			log.Println("连接失败，将在5秒后重试:", err)
			time.Sleep(5 * time.Second)
			continue
		}
		log.Println("连接成功，等待服务器指令...")
		// 连接成功后，重置会话状态
		inSession.Store(false)
		err = listenForCommands(c)
		log.Printf("与服务器的连接已断开: %v. 准备重连...", err)
		c.Close()
	}
}

// listenForCommands 增加会话状态检查
func listenForCommands(c *websocket.Conn) error {
	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			return err
		}
		var cmd ControlMessage
		if err := json.Unmarshal(message, &cmd); err != nil {
			log.Printf("收到无法解析的消息: %s", message)
			continue
		}

		if cmd.Type == "start_session" {
			// MODIFIED: 检查当前是否已在会话中
			// 使用 CompareAndSwap 来原子性地检查和设置状态，防止竞态条件
			if !inSession.CompareAndSwap(false, true) {
				log.Println("已在会话中，忽略新的 'start_session' 指令。")
				continue
			}

			log.Println("收到 'start_session' 指令，正在启动 PTY...")
			handlePtySession(c)

			// 会话结束后，重置状态
			inSession.Store(false)
			log.Println("PTY 会话已结束。返回等待指令状态。")
		}
	}
}

// handlePtySession 保持不变
func handlePtySession(c *websocket.Conn) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := exec.Command("bash", "-i")
	cmd.Env = append(os.Environ(), "TERM=xterm")
	ptmx, err := pty.Start(cmd)
	if err != nil {
		log.Printf("启动 pty 失败: %v", err)
		return
	}
	defer ptmx.Close()

	ptmx.Write([]byte("stty echo\n"))

	go func() {
		defer cancel()
		wsWriter := &websocketWriter{conn: c}
		if _, err := io.Copy(wsWriter, ptmx); err != nil {
			if err != io.EOF {
				log.Printf("从 pty 复制到 websocket 时出错: %v", err)
			}
		}
		log.Println("PTY -> WebSocket 转发协程已结束。")
	}()

	go func() {
		defer cancel()
		for {
			mt, message, err := c.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Printf("读取 WebSocket 消息时出错: %v", err)
				}
				return
			}
			if mt == websocket.TextMessage || mt == websocket.BinaryMessage {
				var resizeMessage ResizeMessage
				if json.Unmarshal(message, &resizeMessage) == nil && resizeMessage.Cols > 0 {
					if err := pty.Setsize(ptmx, &pty.Winsize{Rows: resizeMessage.Rows, Cols: resizeMessage.Cols}); err != nil {
						log.Printf("调整 pty 大小时出错: %v", err)
					}
				} else {
					if _, err := ptmx.Write(message); err != nil {
						log.Printf("写入 pty 时出错: %v", err)
						return
					}
				}
			}
		}
	}()

	<-ctx.Done()
	cmd.Wait()
}

// ... wait 和 websocketWriter 保持不变 ...
func wait(cmd *exec.Cmd) chan error {
	ch := make(chan error, 1)
	go func() {
		ch <- cmd.Wait()
		close(ch)
	}()
	return ch
}

type websocketWriter struct {
	conn *websocket.Conn
}

func (w *websocketWriter) Write(p []byte) (int, error) {
	err := w.conn.WriteMessage(websocket.BinaryMessage, p)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}
