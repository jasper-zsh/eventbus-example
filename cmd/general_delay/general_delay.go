package main

import (
	"context"
	"errors"
	"eventbus-example/general_delay"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"log"
)

var logger = watermill.NewStdLogger(false, false)

const (
	topic       = "example.general_delay"
	handlerName = "example"
)

func main() {
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}
	pubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)

	m, err := general_delay.NewDelayRetry(router, pubSub, pubSub, handlerName, topic, 5)
	m.MaxRetries = 3
	if err != nil {
		panic(err)
	}
	router.AddMiddleware(m.Middleware)
	router.AddNoPublisherHandler(handlerName, topic, pubSub, func(msg *message.Message) error {
		log.Printf("message %s retries %s", msg.UUID, msg.Metadata.Get("retries"))
		return errors.New("always fail")
	})

	go pub(pubSub)

	ctx := context.Background()
	if err := router.Run(ctx); err != nil {
		panic(err)
	}
}

func pub(publisher message.Publisher) {
	for i := 0; i < 2; i++ {
		publisher.Publish(topic, message.NewMessage(watermill.NewUUID(), nil))
	}
}
