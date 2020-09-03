package adapter

import "encoding/json"

type Message struct {
	topic string
	data  interface{}
}

// MsgPair
type MsgPair interface {
	// Topic 返回消息所属主题
	Topic() string
	// Data 返回消息的内容
	Data() interface{}
	// Ack acknowledge
	Ack() error
	// Nack not acknowledge
	Nack() error
}

// Marshal 序列化消息
func Marshal(message interface{}) (buff []byte, err error) {
	switch message.(type) {
	case []byte:
		return message.([]byte), nil
	case string:
		buff = []byte(message.(string))
	default:
		buff, err = json.Marshal(message)
		return
	}
	return
}