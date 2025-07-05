// File: cmd/agent/main.go
package main

import (
	"flag"
	"log"

	"go-remote-shell/internal/agent" // 导入我们自己的 agent 包
)

func main() {
	// 1. 定义和解析命令行参数
	serverAddr := flag.String("addr", "10.0.0.178:3000", "WebSocket server address")
	clientId := flag.String("id", "golang-client-1", "Client ID for this agent")
	dbPath := flag.String("db", "users.json", "用户凭据数据库文件路径")

	// FIDO2/WebAuthn 需要知道谁是“信赖方”(Relying Party)
	// 在生产环境中，这必须是提供前端页面的域名 (例如 "example.com")
	// 浏览器也要求必须在 HTTPS 或 localhost 下使用 WebAuthn
	rpID := flag.String("rpid", "localhost", "Relying Party ID (必须是域名)")
	rpOrigin := flag.String("origin", "http://localhost:3000", "Relying Party Origin (前端页面源)")

	flag.Parse()

	log.Println("--- Go Remote Shell Agent ---")

	// 2. 创建 Agent 核心实例
	// 注意：这里的 rpID 和 rpOrigin 对于 FIDO2 至关重要
	app, err := agent.New(*clientId, *serverAddr, *dbPath, *rpID, *rpOrigin)
	if err != nil {
		log.Fatalf("无法初始化 Agent: %v", err)
	}

	// 3. 运行 Agent 的主循环
	app.Run()
}
