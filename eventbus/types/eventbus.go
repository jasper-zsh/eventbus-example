package types

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type SubscriberFactory func(topic, handlerName string, logger watermill.LoggerAdapter) (message.Subscriber, error)
