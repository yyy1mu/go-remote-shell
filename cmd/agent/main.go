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
	"os/signal"
	"syscall"

	"github.com/creack/pty"
	"github.com/gorilla/websocket"
	"golang.org/x/term"
)

// ResizeMessage 定义了用于调整 PTY 大小的消息结构
// 通常由 WebSocket 服务器发送而来
type ResizeMessage struct {
	Cols uint16 `json:"cols"`
	Rows uint16 `json:"rows"`
}

var (
	serverAddr = flag.String("addr", "10.0.0.178:3000", "WebSocket server address")
	clientId   = flag.String("id", "golang-client-1", "Client ID for this agent")
)

func main() {
	// 解析命令行参数
	flag.Parse()

	// 监听中断信号，用于程序退出时进行清理
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	// 构建 WebSocket 连接 URL
	// 包含了 type=agent 和 clientId 参数，用于向服务器表明身份
	u := url.URL{Scheme: "ws", Host: *serverAddr, Path: "/ws", RawQuery: "type=agent&clientId=" + *clientId}
	log.Printf("正在连接到 %s", u.String())

	// 使用 gorilla/websocket 库建立连接
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("连接失败:", err)
	}
	defer c.Close()

	log.Println("连接成功!")

	// --- PTY 和 Shell 设置 ---
	cmd := exec.Command("bash", "-i")
	cmd.Env = append(os.Environ(), "TERM=xterm")
	ptmx, err := pty.Start(cmd)
	if err != nil {
		log.Fatalf("启动 pty 失败: %v", err)
	}
	defer ptmx.Close()

	// --- 终端原始模式设置 ---
	// MakeRaw will also handle restoring the state on exit.
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		log.Fatalf("设置终端为原始模式失败: %v", err)
	}
	defer term.Restore(int(os.Stdin.Fd()), oldState)

	// ---- **** 这是画龙点睛的修改 **** ----
	// 在 bash 启动后，立即发送 stty echo 命令来确保回显是开启的
	_, err = ptmx.Write([]byte("stty echo\n"))
	if err != nil {
		log.Fatalf("写入 stty echo 失败: %v", err)
	}
	// ---- **** 修改部分结束 **** ----

	// --- 双向数据转发 ---
	// PTY -> WebSocket
	go func() {
		wsWriter := &websocketWriter{conn: c}
		// io.Copy is blocking, so it runs in a goroutine.
		if _, err := io.Copy(wsWriter, ptmx); err != nil {
			if err != io.EOF {
				log.Printf("从 pty 读取数据时出错: %v", err)
			}
		}
		log.Println("PTY -> WebSocket 的转发协程已结束。")
	}()

	// WebSocket -> PTY
	go func() {
		defer ptmx.Close()
		defer c.Close()
		for {
			mt, message, err := c.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Printf("读取 WebSocket 消息时出错: %v", err)
				} else {
					log.Println("WebSocket 连接已关闭。")
				}
				break
			}

			if mt == websocket.TextMessage || mt == websocket.BinaryMessage {
				var resizeMessage ResizeMessage
				if json.Unmarshal(message, &resizeMessage) == nil && resizeMessage.Cols > 0 {
					log.Printf("收到窗口大小调整命令: %dx%d", resizeMessage.Cols, resizeMessage.Rows)
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
		log.Println("WebSocket -> PTY 的转发协程已结束。")
	}()

	// 监听本地终端窗口大小变化 (SIGWINCH)
	sigwinch := make(chan os.Signal, 1)
	signal.Notify(sigwinch, syscall.SIGWINCH)
	go func() {
		for range sigwinch {
			if err := pty.InheritSize(os.Stdin, ptmx); err != nil {
				log.Printf("继承窗口大小时出错: %v", err)
			}
		}
	}()
	sigwinch <- syscall.SIGWINCH // Initial resize.

	// 等待 shell 进程结束或中断信号
	select {
	case <-interrupt:
		log.Println("收到中断信号，正在退出...")
	case err := <-wait(cmd):
		if err != nil {
			log.Printf("Shell 进程已结束，错误: %v", err)
		} else {
			log.Println("Shell 进程已结束。")
		}
	}
}

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

func (w *websocketWriter) Write(p []byte) (int, error) {
	err := w.conn.WriteMessage(websocket.BinaryMessage, p)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}
