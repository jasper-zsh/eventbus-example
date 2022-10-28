package eventbus

import "github.com/ThreeDotsLabs/watermill/message"

type DualChannelMiddleware struct {
	externalPublisher message.Publisher
}
