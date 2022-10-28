package general_delay

import (
	"eventbus-example/eventbus/types"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"log"
	"strconv"
	"time"
)

type DelayRetryConfig struct {
	MaxRetries     int
	DelaySeconds   int64
	Topic          string
	ConsumerPrefix string
	HandlerName    string
}

type DelayRetry struct {
	logger     watermill.LoggerAdapter
	config     DelayRetryConfig
	delayPub   message.Publisher
	delaySub   message.Subscriber
	publisher  message.Publisher
	router     *message.Router
	deadTopic  string
	delayTopic string
}

const (
	headerNextTick = "Next-Tick"
	headerRetries  = "Retries"
)

func NewDelayRetry(router *message.Router, publisher message.Publisher, subscriberFactory types.SubscriberFactory, config DelayRetryConfig, logger watermill.LoggerAdapter) (*DelayRetry, error) {
	r := &DelayRetry{
		logger:    logger,
		config:    config,
		delayPub:  publisher,
		publisher: publisher,
		router:    router,
	}
	r.delayTopic = fmt.Sprintf("%s.%s.delay_%ds", config.Topic, config.HandlerName, config.DelaySeconds)
	r.deadTopic = fmt.Sprintf("%s.%s.dead", config.Topic, config.HandlerName)
	var err error
	r.delaySub, err = subscriberFactory(r.delayTopic, "retry", logger)
	if err != nil {
		return nil, err
	}
	router.AddNoPublisherHandler(fmt.Sprintf("%s:%s", config.ConsumerPrefix, r.delayTopic), r.delayTopic, r.delaySub, func(msg *message.Message) error {
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
		if r.config.MaxRetries > 0 && retries >= r.config.MaxRetries {
			log.Printf("message %s out of retries, published to dead topic.", msg.UUID)
			err := r.publisher.Publish(r.deadTopic, msg)
			if err != nil {
				return err
			}
			return nil
		}
		msg.Metadata.Set(headerRetries, strconv.Itoa(retries+1))

		return r.publisher.Publish(r.config.Topic, msg)
	})
	return r, nil
}

func (r DelayRetry) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		producedMessages, err := h(msg)
		if err == nil {
			return producedMessages, nil
		}

		msg.Metadata.Set(headerNextTick, strconv.FormatInt(time.Now().Unix()+r.config.DelaySeconds, 10))
		return nil, r.delayPub.Publish(r.delayTopic, msg)
	}
}
