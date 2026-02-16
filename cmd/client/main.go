package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	url := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(url)
	if err != nil {
		conn.Close()
		return
	}
	defer conn.Close()

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println(err)
		return
	}
	s := []string{routing.PauseKey, username}
	queueName := strings.Join(s, ".")

	gameState := gamelogic.NewGameState(username)

	pubsub.SubscribeJSON(conn,
		routing.ExchangePerilDirect,
		queueName,
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gameState))

	s = []string{routing.ArmyMovesPrefix, username}
	movesQueue := strings.Join(s, ".")
	pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, movesQueue, "army_moves.*",
		pubsub.Transient, handlerMove(gameState))

	publishChannel, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	for {
		input := gamelogic.GetInput()

		switch input[0] {
		case "spawn":
			err := gameState.CommandSpawn(input)
			if err != nil {
				fmt.Println(err)
				continue
			}

		case "move":
			mv, err := gameState.CommandMove(input)
			if err != nil {
				fmt.Println(err)
				continue
			}
			routingKey := routing.ArmyMovesPrefix + "." + mv.Player.Username
			err = pubsub.PublishJSON(publishChannel,
				routing.ExchangePerilTopic,
				routingKey, mv)
			if err != nil {
				fmt.Printf("error publishing move: %s\n", err)
			}
			fmt.Printf("Published move for %s\n", mv.Player.Username)

		case "status":
			gameState.CommandStatus()

		case "help":
			gamelogic.PrintClientHelp()

		case "spam":
			fmt.Println("Spamming not allowed yet!")

		case "quit":
			gamelogic.PrintQuit()
			return

		default:
			fmt.Println("unkown command")
			continue
		}

	}

	// signalChan := make(chan os.Signal, 1)
	// signal.Notify(signalChan, os.Interrupt)
	// <-signalChan
	// fmt.Println("... shutting client down...")
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	defer fmt.Print("> ")
	return func(ps routing.PlayingState) {
		gs.HandlePause(ps)
	}

}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
	return func(move gamelogic.ArmyMove) {
		defer fmt.Print("> ")
		gs.HandleMove(move)
	}
}
