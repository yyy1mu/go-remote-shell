// File: internal/protocol/protocol.go
package protocol

import (
	"encoding/base64"
	"encoding/json"
)

// Message 定义了 Server 和 Agent 之间通信的统一消息结构
type Message struct {
	Type      string `json:"type"`                // 消息类型: "start_session", "data", "resize", "close_session", 等
	SessionID string `json:"sessionId,omitempty"` // 唯一的会话标识符 (登录成功后使用)
	Payload   string `json:"payload,omitempty"`   // 对于 "data", 这是 base64 编码后的数据; 对于 FIDO2, 这是序列化的 JSON
	User      string `json:"user,omitempty"`      // 用户名
	Error     string `json:"error,omitempty"`     // 用于传递错误信息
	Cols      uint16 `json:"cols,omitempty"`      // 对于 "resize"
	Rows      uint16 `json:"rows,omitempty"`      // 对于 "resize"
}

// FidoPayload 用于在 Payload 字段中传递 FIDO2 相关的 JSON 数据
type FidoPayload struct {
	CredentialCreation *json.RawMessage `json:"credentialCreation,omitempty"`
	CredentialRequest  *json.RawMessage `json:"credentialRequest,omitempty"`
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
