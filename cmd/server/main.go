package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	url := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(url)
	if err != nil {
		conn.Close()
		return
	}
	defer conn.Close()
	fmt.Println("Connection Successful")

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return
	}

	err = pubsub.SubscribeGob(conn, routing.ExchangePerilTopic, routing.GameLogSlug, routing.GameLogSlug+".*", pubsub.Durable, handlerLogs())
	if err != nil {
		fmt.Println(err)
		return
	}

	gamelogic.PrintServerHelp()

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		} else if input[0] == "pause" {
			fmt.Println("sending pause message...")
			err = pubsub.PublishJSON(ch, string(routing.ExchangePerilDirect), string(routing.PauseKey), routing.PlayingState{IsPaused: true})
			if err != nil {
				fmt.Println("Error pausing the game")
				return
			}
		} else if input[0] == "resume" {
			fmt.Println("sending resume message...")
			err = pubsub.PublishJSON(ch, string(routing.ExchangePerilDirect), string(routing.PauseKey), routing.PlayingState{IsPaused: false})
			if err != nil {
				fmt.Println("Error pausing the game")
				return
			}
		} else if input[0] == "quit" {
			fmt.Println("exiting...")
			return
		} else {
			fmt.Println("Unknown command...")
		}

	}

}

func handlerLogs() func(routing.GameLog) pubsub.AckType {
	return func(glog routing.GameLog) pubsub.AckType {
		defer fmt.Print("> ")
		gamelogic.WriteLog(glog)
		return pubsub.Ack
	}
}
