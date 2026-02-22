package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

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

	publishChannel, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	s = []string{routing.ArmyMovesPrefix, username}
	movesQueue := strings.Join(s, ".")
	pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, movesQueue, "army_moves.*",
		pubsub.Transient, handlerMove(gameState, publishChannel))

	pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, "war", routing.WarRecognitionsPrefix+".*", pubsub.Durable, handlerWar(gameState, publishChannel))

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
			if len(input) > 1 {
				n, err := strconv.Atoi(input[1])
				if err != nil {
					fmt.Println(err)
					continue
				}

				for range n {
					mlog := gamelogic.GetMaliciousLog()
					err = publishGameLog(publishChannel, gameState.GetUsername(), mlog)
					if err != nil {
						fmt.Println(err)
					}
				}
				fmt.Printf("Published %v malicious logs\n", n)
			} else {
				fmt.Println("usage: spam <number>")
			}

		case "quit":
			gamelogic.PrintQuit()
			return

		default:
			fmt.Println("unkown command")
			continue
		}

	}

}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	defer fmt.Print("> ")
	return func(ps routing.PlayingState) pubsub.AckType {
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, publishCh *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		mvOucome := gs.HandleMove(move)
		switch mvOucome {
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.Ack
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			key := routing.WarRecognitionsPrefix + "." + move.Player.Username
			err := pubsub.PublishJSON(publishCh, routing.ExchangePerilTopic, key, gamelogic.RecognitionOfWar{
				Attacker: move.Player,
				Defender: gs.GetPlayerSnap(),
			})
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}
		return pubsub.NackDiscard
	}
}

func handlerWar(gs *gamelogic.GameState, publishCh *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {

	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")

		outcome, w, l := gs.HandleWar(rw)
		fmt.Println("war outcome:", outcome, "winner:", w, "loser:", l)

		winMessage := w + " won a war against " + l
		drawMessage := "A war between " + w + " and " + l + " resulted in a draw "

		// TODO: switch on outcome and return the appropriate AckType
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			if err := publishGameLog(publishCh, gs.GetUsername(), winMessage); err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			if err := publishGameLog(publishCh, gs.GetUsername(), winMessage); err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			if err := publishGameLog(publishCh, gs.GetUsername(), drawMessage); err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			fmt.Println("unknown outcome")
			return pubsub.NackDiscard
		}

	}
}

func publishGameLog(publishCh *amqp.Channel, username, message string) error {
	log := routing.GameLog{
		CurrentTime: time.Now(),
		Message:     message,
		Username:    username,
	}

	return pubsub.PublishGob(publishCh, routing.ExchangePerilTopic, routing.GameLogSlug+"."+username, log)

}
