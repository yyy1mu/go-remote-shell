// File: cmd/agent/main.go
package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/url"
	"os"
	"os/exec"
	"time"

	"github.com/creack/pty"
	"github.com/gorilla/websocket"
)

// ResizeMessage 定义了用于调整 PTY 大小的消息结构
type ResizeMessage struct {
	Cols uint16 `json:"cols"`
	Rows uint16 `json:"rows"`
}

// ControlMessage 用于接收来自服务器的指令
type ControlMessage struct {
	Type string `json:"type"`
}

var (
	serverAddr = flag.String("addr", "10.0.0.178:3000", "WebSocket server address")
	clientId   = flag.String("id", "golang-client-1", "Client ID for this agent")
)

// main 函数现在负责保持与服务器的连接，并等待指令
func main() {
	flag.Parse()
	log.Printf("Agent启动，ID: %s, 准备连接到服务器: %s", *clientId, *serverAddr)

	u := url.URL{Scheme: "ws", Host: *serverAddr, Path: "/ws", RawQuery: "type=agent&clientId=" + *clientId}

	// 外部循环，用于处理断线重连
	for {
		log.Printf("正在连接到 %s", u.String())
		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		if err != nil {
			log.Println("连接失败，将在5秒后重试:", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Println("连接成功，等待服务器指令...")

		// 内部循环，处理来自服务器的消息
		err = listenForCommands(c)

		// 如果 listenForCommands 返回，说明连接已断开
		log.Printf("与服务器的连接已断开: %v. 准备重连...", err)
		c.Close() // 确保关闭旧的连接
	}
}

// listenForCommands 监听来自 WebSocket 的指令
func listenForCommands(c *websocket.Conn) error {
	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			return err // 连接错误，返回到上层进行重连
		}

		var cmd ControlMessage
		if err := json.Unmarshal(message, &cmd); err != nil {
			log.Printf("收到无法解析的消息: %s", message)
			continue
		}

		// 如果是启动会话的指令，则开始 PTY 会话
		if cmd.Type == "start_session" {
			log.Println("收到 'start_session' 指令，正在启动 PTY...")
			// handlePtySession 是一个阻塞函数，它会处理整个 PTY 会话的生命周期
			// 直到会话结束（例如，用户退出或连接断开）
			handlePtySession(c)
			log.Println("PTY 会话已结束。返回等待指令状态。")
		}
	}
}

// handlePtySession 负责创建和管理一个 PTY 会话的完整生命周期
func handlePtySession(c *websocket.Conn) {
	// --- PTY 和 Shell 设置 ---
	cmd := exec.Command("bash", "-i")
	cmd.Env = append(os.Environ(), "TERM=xterm")
	ptmx, err := pty.Start(cmd)
	if err != nil {
		log.Printf("启动 pty 失败: %v", err)
		return
	}
	defer ptmx.Close()

	// 确保 stty echo 开启
	ptmx.Write([]byte("stty echo\n"))

	// --- 双向数据转发 ---
	// 创建一个 channel 来通知会话结束
	done := make(chan struct{})

	// Goroutine: PTY -> WebSocket
	go func() {
		defer close(done) // 当这个 goroutine 结束时，关闭 done channel
		wsWriter := &websocketWriter{conn: c}
		if _, err := io.Copy(wsWriter, ptmx); err != nil && err != io.EOF {
			log.Printf("从 pty 复制到 websocket 时出错: %v", err)
		}
		log.Println("PTY -> WebSocket 转发协程已结束。")
	}()

	// Goroutine: WebSocket -> PTY
	go func() {
		// 当这个 goroutine 结束时，关闭 pty 来终止 bash 进程
		defer ptmx.Close()
		for {
			mt, message, err := c.ReadMessage()
			if err != nil {
				// 如果读取出错（通常意味着连接已断开），则退出循环
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Printf("读取 WebSocket 消息时出错: %v", err)
				}
				break
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
						break
					}
				}
			}
		}
		log.Println("WebSocket -> PTY 转发协程已结束。")
	}()

	// 等待 shell 进程自己退出，或者等待 PTY->WebSocket 的转发协程结束
	// 任何一个结束都意味着会话应该终结
	select {
	case <-done: // PTY -> WebSocket 的转发已停止 (通常因为 pty 关闭)
	case err := <-wait(cmd): // Shell 进程自己退出了 (例如用户输入了 exit)
		if err != nil {
			log.Printf("Shell 进程已结束，错误: %v", err)
		} else {
			log.Println("Shell 进程已正常结束。")
		}
	}

	// 会话结束，函数返回
}

// wait 辅助函数，用于等待命令执行完成
func wait(cmd *exec.Cmd) chan error {
	ch := make(chan error, 1)
	go func() {
		ch <- cmd.Wait()
		close(ch)
	}()
	return ch
}

// websocketWriter 是一个辅助结构，它实现了 io.Writer 接口
type websocketWriter struct {
	conn *websocket.Conn
}

// Write 将字节切片作为 WebSocket 二进制消息写入
func (w *websocketWriter) Write(p []byte) (int, error) {
	err := w.conn.WriteMessage(websocket.BinaryMessage, p)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}
