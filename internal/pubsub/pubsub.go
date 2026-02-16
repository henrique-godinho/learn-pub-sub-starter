package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonData, err := json.Marshal(val)
	if err != nil {
		fmt.Println("error parsing json")
		return err
	}
	ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        jsonData,
		},
	)
	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {

	ch, err := conn.Channel()
	if err != nil {
		ch.Close()
		return &amqp.Channel{}, amqp.Queue{}, err
	}

	durable := false
	if queueType == 0 {
		durable = true
	}
	autoDelete := false
	if queueType == 1 {
		autoDelete = true
	}

	exclusive := false

	q, err := ch.QueueDeclare(
		queueName, durable, autoDelete, exclusive, false, nil)
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, err
	}
	if err := ch.QueueBind(q.Name, key, exchange, false, nil); err != nil {
		return nil, amqp.Queue{}, err
	}
	return ch, q, nil

}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T),
) error {
	ch, q, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	delivery, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for message := range delivery {
			var v T
			err := json.Unmarshal(message.Body, &v)
			if err != nil {
				fmt.Println("error unmarshaling")
				continue
			}
			handler(v)
			_ = message.Ack(false)
		}
	}()
	return nil
}
