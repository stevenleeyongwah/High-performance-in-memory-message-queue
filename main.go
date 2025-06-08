// main.go
package main

import (
	"fmt"
	"time"
)

func main() {
	broker := NewBroker()

	// Subscribe to "chat" topic
	sub := broker.Subscribe("chat", "user1")

	// Start a goroutine to listen for messages
	go func() {
		for msg := range sub.Ch {
			fmt.Printf("[user1 received] %s\n", string(msg.Payload))
		}
	}()

	// Publish a message
	broker.Publish("chat", Message{
		Topic:   "chat",
		Payload: []byte("Hello, world!"),
	})

	time.Sleep(time.Second)
}
