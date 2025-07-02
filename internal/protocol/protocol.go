package protocol

import "encoding/base64"

// Message 定义了 Server 和 Agent 之间通信的统一消息结构
type Message struct {
	Type      string `json:"type"`                // 消息类型: "start_session", "data", "resize", "close_session"
	SessionID string `json:"sessionId,omitempty"` // 唯一的会话标识符
	Payload   string `json:"payload,omitempty"`   // 对于 "data" 类型, 这是 base64 编码后的数据
	User      string `json:"user,omitempty"`      // 对于 "start_session", 这是要启动的用户名
	Cols      uint16 `json:"cols,omitempty"`      // 对于 "resize", 这是列数
	Rows      uint16 `json:"rows,omitempty"`      // 对于 "resize", 这是行数
}

// DecodePayload 将 base64 编码的 payload 解码为字节切片
func (m *Message) DecodePayload() ([]byte, error) {
	return base64.StdEncoding.DecodeString(m.Payload)
}

// NewDataMessage 创建一个新的 "data" 类型的消息
func NewDataMessage(sessionID string, data []byte) Message {
	return Message{
		Type:      "data",
		SessionID: sessionID,
		Payload:   base64.StdEncoding.EncodeToString(data),
	}
}
