package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
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
	queueType routing.SimpleQueueType, // an enum to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {

	ch, err := conn.Channel()
	if err != nil {
		ch.Close()
		return &amqp.Channel{}, amqp.Queue{}, err
	}
	defer ch.Close()

	durable := false
	if queueType == 0 {
		durable = true
	}
	autoDelete := false
	if queueType == 1 {
		autoDelete = true
	}

	exclusive := false
	if queueType == 1 {
		exclusive = true
	}

	q, err := ch.QueueDeclare(
		queueName, durable, autoDelete, exclusive, false, nil)
	if err != nil {
		ch.Close()
		return &amqp.Channel{}, amqp.Queue{}, err
	}
	return ch, q, nil

}
