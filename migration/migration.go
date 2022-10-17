package migration

import (
	"context"
	"encoding/json"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"log"
)

type OldSchema struct {
	OldField int
}

type NewSchema struct {
	OldField int
	NewField int
}

const topic = "example.migrate"

func RunMigrationRouter(logger watermill.LoggerAdapter) (*message.Router, error) {
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		return nil, err
	}
	pubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)
	handler := router.AddHandler("migratedHandler", topic, pubSub, "", nil, func(msg *message.Message) ([]*message.Message, error) {
		p := NewSchema{}
		err := json.Unmarshal(msg.Payload, &p)
		if err != nil {
			return nil, err
		}
		log.Printf("[%s] %+v", msg.UUID, p)
		return nil, nil
	})
	handler.AddMiddleware(func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			// 为了提高性能，可以用Metadata区分版本，只处理指定版本的消息
			p := make(map[string]interface{})
			err := json.Unmarshal(msg.Payload, &p)
			if err != nil {
				return nil, err
			}
			if _, ok := p["NewField"]; !ok {
				// json.Unmarshal默认会把所有数字类型反序列化为float64
				o, ok := p["OldField"].(float64)
				if ok {
					p["NewField"] = o + 1
				}
			}
			converted, err := json.Marshal(p)
			if err != nil {
				return nil, err
			}
			msg.Payload = converted
			return h(msg)
		}
	})
	go pub(pubSub)
	ctx := context.Background()
	if err := router.Run(ctx); err != nil {
		panic(err)
	}
	return router, nil
}

func pub(publisher message.Publisher) {
	p1, _ := json.Marshal(OldSchema{
		OldField: 1,
	})
	publisher.Publish(topic, message.NewMessage(watermill.NewUUID(), p1))
	p2, _ := json.Marshal(NewSchema{
		OldField: 2,
		NewField: 3,
	})
	publisher.Publish(topic, message.NewMessage(watermill.NewUUID(), p2))
}
