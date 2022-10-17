package general_delay

import (
	"context"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"log"
	"strconv"
	"time"
)

type DelayRetry struct {
	pubDelay5s message.Publisher
	subDelay5s message.Subscriber
	publisher  message.Publisher
	topic      string
	router     *message.Router
	MaxRetries int
}

const (
	headerNextTick = "next_tick"
	headerRetries  = "retries"
	topicDelay5s   = "delay_5s"
)

var logger = watermill.NewStdLogger(false, false)

func NewDelayRetry(publisher message.Publisher, topic string) (*DelayRetry, error) {
	pubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		return nil, err
	}
	r := &DelayRetry{
		pubDelay5s: pubSub,
		subDelay5s: pubSub,
		publisher:  publisher,
		topic:      topic,
		router:     router,
	}
	router.AddNoPublisherHandler("delay_5s", topicDelay5s, r.subDelay5s, func(msg *message.Message) error {
		nextTickStr := msg.Metadata.Get(headerNextTick)
		if nextTickStr != "" {
			nextTick, _ := strconv.ParseInt(nextTickStr, 10, 64)
			if wait := nextTick - time.Now().Unix(); wait > 0 {
				time.Sleep(time.Duration(wait) * time.Second)
			}
		}
		retriesStr := msg.Metadata.Get(headerRetries)
		if retriesStr == "" {
			retriesStr = "0"
		}
		retries, _ := strconv.Atoi(retriesStr)
		if retries >= r.MaxRetries {
			log.Printf("message %s out of retries, dropped.", msg.UUID)
			return nil
		}
		msg.Metadata.Set(headerRetries, strconv.Itoa(retries+1))

		return r.publisher.Publish(r.topic, msg)
	})
	go router.Run(context.Background())
	return r, nil
}

func (r DelayRetry) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		producedMessages, err := h(msg)
		if err == nil {
			return producedMessages, nil
		}

		msg.Metadata.Set(headerNextTick, strconv.FormatInt(time.Now().Unix()+5, 10))
		return nil, r.pubDelay5s.Publish(topicDelay5s, msg)
	}
}
