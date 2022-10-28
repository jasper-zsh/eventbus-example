package main

import (
	"context"
	"encoding/json"
	"errors"
	"eventbus-example/eventbus"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"time"
)

var logger = watermill.NewStdLogger(false, false)

type LicenseAdded struct {
	OrgUUID string
}

func main() {
	brokers := []string{"127.0.0.1:9092"}
	publisher, err := eventbus.NewKafkaPublisher(brokers, 10, logger)
	subscriberFactory, err := eventbus.NewKafkaSubscriberFactory(brokers, 10)
	if err != nil {
		panic(err)
	}
	bus, err := eventbus.NewEventBus(publisher, subscriberFactory, &eventbus.Config{
		InstanceID:        "eventbus-example",
		RetryDelaySeconds: 3,
		MaxRetries:        0,
	}, logger)
	event := LicenseAdded{}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)
	msg.Metadata.Set("Routing-Key", event.OrgUUID)

	bus.Subscribe("ones.license.added", "send_license_email", func(msg *message.Message) error {
		payload := LicenseAdded{}
		_ = json.Unmarshal(msg.Payload, &payload)
		// do something
		fmt.Printf("Received: [%s] (%+v) %+v\n", msg.UUID, msg.Metadata, payload)
		return errors.New("xxx")
		// an error occured
		if err != nil {
			return err
		}
		// success
		return nil
	})
	go bus.Run(context.Background())

	time.Sleep(time.Second * 5)

	bus.Publish("ones.license.added", msg)

	time.Sleep(time.Second * 30)
}
