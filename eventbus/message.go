package eventbus

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

const (
	HeaderRoutingKey = "Routing-Key"
)

func NewMessage(routingKey string, payload []byte) *message.Message {
	msg := message.NewMessage(watermill.NewUUID(), payload)
	msg.Metadata.Set(HeaderRoutingKey, routingKey)
	return msg
}
