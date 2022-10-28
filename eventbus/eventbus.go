package eventbus

import (
	"context"
	"eventbus-example/eventbus/types"
	"eventbus-example/general_delay"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Config struct {
	InstanceID        string
	RetryDelaySeconds int64
	MaxRetries        int
}

func newDefaultEventBusConfig() Config {
	return Config{
		RetryDelaySeconds: 30,
		MaxRetries:        5,
	}
}

type EventBus struct {
	config            Config
	logger            watermill.LoggerAdapter
	Publisher         message.Publisher
	SubscriberFactory types.SubscriberFactory
	Router            *message.Router
}

func NewEventBus(publisher message.Publisher, subscriberFactory types.SubscriberFactory, config *Config, logger watermill.LoggerAdapter) (*EventBus, error) {
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		return nil, err
	}
	bus := &EventBus{
		logger:            logger,
		Publisher:         publisher,
		SubscriberFactory: subscriberFactory,
		Router:            router,
	}
	if config == nil {
		bus.config = newDefaultEventBusConfig()
	} else {
		bus.config = *config
	}
	return bus, nil
}

func (bus *EventBus) Run(ctx context.Context) error {
	return bus.Router.Run(ctx)
}

func (bus *EventBus) Publish(topic string, messages ...*message.Message) error {
	err := bus.Publisher.Publish(topic, messages...)
	if err != nil {
		return err
	}
	return nil
}

func (bus *EventBus) Subscribe(topic, handlerName string, handlerFunc message.NoPublishHandlerFunc) error {
	consumerPrefix := fmt.Sprintf("%s:%s", bus.config.InstanceID, topic)
	subscriber, err := bus.SubscriberFactory(topic, handlerName, bus.logger)
	if err != nil {
		return err
	}
	handler := bus.Router.AddHandler(fmt.Sprintf("%s:%s", consumerPrefix, handlerName), topic, subscriber, "", disabledPublisher{}, func(msg *message.Message) ([]*message.Message, error) {
		return nil, handlerFunc(msg)
	})
	retry, err := general_delay.NewDelayRetry(bus.Router, bus.Publisher, bus.SubscriberFactory, general_delay.DelayRetryConfig{
		Topic:          topic,
		HandlerName:    handlerName,
		ConsumerPrefix: consumerPrefix,
		DelaySeconds:   bus.config.RetryDelaySeconds,
		MaxRetries:     bus.config.MaxRetries,
	}, bus.logger)
	if err != nil {
		return err
	}
	handler.AddMiddleware(retry.Middleware)
	return nil
}

type disabledPublisher struct{}

func (disabledPublisher) Publish(topic string, messages ...*message.Message) error {
	return message.ErrOutputInNoPublisherHandler
}

func (disabledPublisher) Close() error {
	return nil
}
