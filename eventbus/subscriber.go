package eventbus

import (
	"eventbus-example/eventbus/types"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

func NewKafkaSubscriberFactory(brokers []string, initialPartitions int32) (types.SubscriberFactory, error) {
	topicCreator, err := newKafkaTopicCreator(brokers, initialPartitions)
	if err != nil {
		return nil, err
	}
	return func(topic, handlerName string, logger watermill.LoggerAdapter) (message.Subscriber, error) {
		err := topicCreator.TryCreateTopic(topic)
		if err != nil {
			return nil, err
		}
		saramaConfig := kafka.DefaultSaramaSubscriberConfig()
		saramaConfig.Metadata.AllowAutoTopicCreation = false
		return kafka.NewSubscriber(kafka.SubscriberConfig{
			Brokers:               brokers,
			Unmarshaler:           kafka.DefaultMarshaler{},
			ConsumerGroup:         handlerName,
			OverwriteSaramaConfig: saramaConfig,
		}, logger)
	}, nil
}
