package eventbus

import (
	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

func NewKafkaPublisher(brokers []string, initialPartitions int32, logger watermill.LoggerAdapter) (message.Publisher, error) {
	publisher, err := kafka.NewPublisher(kafka.PublisherConfig{
		Brokers: brokers,
		Marshaler: kafka.NewWithPartitioningMarshaler(func(topic string, msg *message.Message) (string, error) {
			routingKey := msg.Metadata.Get(HeaderRoutingKey)
			if len(routingKey) > 0 {
				return routingKey, nil
			} else {
				return msg.UUID, nil
			}
		}),
	}, logger)
	if err != nil {
		return nil, err
	}
	wrapper, err := newKafkaPublisherWrapper(brokers, publisher, initialPartitions)
	if err != nil {
		return nil, err
	}
	return wrapper, nil
}

var _ message.Publisher = (*KafkaPublisherWrapper)(nil)

type KafkaPublisherWrapper struct {
	topicCreator   *kafkaTopicCreator
	kafkaPublisher *kafka.Publisher
}

func (k KafkaPublisherWrapper) Publish(topic string, messages ...*message.Message) error {
	err := k.topicCreator.TryCreateTopic(topic)
	if err != nil {
		return err
	}
	return k.kafkaPublisher.Publish(topic, messages...)
}

func (k KafkaPublisherWrapper) Close() error {
	return k.kafkaPublisher.Close()
}

type kafkaTopicCreator struct {
	admin             sarama.ClusterAdmin
	topics            map[string]struct{}
	initialPartitions int32
	replicationFactor int
}

func newKafkaTopicCreator(brokers []string, initialPartitions int32) (*kafkaTopicCreator, error) {
	admin, err := sarama.NewClusterAdmin(brokers, nil)
	if err != nil {
		return nil, err
	}
	creator := &kafkaTopicCreator{
		admin:             admin,
		topics:            make(map[string]struct{}),
		initialPartitions: initialPartitions,
	}
	topics, err := admin.ListTopics()
	if err != nil {
		return nil, err
	}
	b, _, err := admin.DescribeCluster()
	if err != nil {
		return nil, err
	}
	if len(b) >= 3 {
		creator.replicationFactor = 3
	} else {
		creator.replicationFactor = len(b)
	}
	for topic, _ := range topics {
		creator.topics[topic] = struct{}{}
	}
	return creator, nil
}

func (c *kafkaTopicCreator) TryCreateTopic(topic string) error {
	if _, ok := c.topics[topic]; !ok {
		err := c.admin.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions:     c.initialPartitions,
			ReplicationFactor: int16(c.replicationFactor),
		}, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func newKafkaPublisherWrapper(brokers []string, publisher *kafka.Publisher, initialPartitions int32) (message.Publisher, error) {
	topicCreator, err := newKafkaTopicCreator(brokers, initialPartitions)
	if err != nil {
		return nil, err
	}
	wrapper := &KafkaPublisherWrapper{
		topicCreator:   topicCreator,
		kafkaPublisher: publisher,
	}

	return wrapper, nil
}
