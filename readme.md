编译项目
```shell
go build -o . ./cmd/server/
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o . ./cmd/agent/.
```

在tool目录下是python脚本，用于处理日志。replay重现shell会话，-o到保存文件。  
analyze用 LLM 分析日志。

session_logs 保存的日志
agent会在本地生成users.json 保存 用户的passkey信息