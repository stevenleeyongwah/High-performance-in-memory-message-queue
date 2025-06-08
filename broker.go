// broker.go
package main

import (
	"sync"
)

// Message represents a message in the queue
type Message struct {
	Topic   string
	Payload []byte
}

// Subscriber receives messages
type Subscriber struct {
	ID string
	Ch chan Message
}

// Topic manages messages and subscribers
type Topic struct {
	Name        string
	Subscribers map[string]*Subscriber
	Messages    chan Message
	mu          sync.RWMutex
}

// Broker manages all topics
type Broker struct {
	Topics map[string]*Topic
	mu     sync.RWMutex
}

func NewBroker() *Broker {
	return &Broker{
		Topics: make(map[string]*Topic),
	}
}

func (b *Broker) Subscribe(topicName, subscriberID string) *Subscriber {
	b.mu.Lock()
	defer b.mu.Unlock()

	topic, ok := b.Topics[topicName]
	if !ok {
		topic = &Topic{
			Name:        topicName,
			Subscribers: make(map[string]*Subscriber),
			Messages:    make(chan Message, 100),
		}
		b.Topics[topicName] = topic
		go topic.broadcast()
	}

	sub := &Subscriber{
		ID: subscriberID,
		Ch: make(chan Message, 10),
	}
	topic.mu.Lock()
	topic.Subscribers[subscriberID] = sub
	topic.mu.Unlock()

	return sub
}

func (b *Broker) Publish(topicName string, msg Message) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	topic, ok := b.Topics[topicName]
	if !ok {
		return
	}

	select {
	case topic.Messages <- msg:
	default:
		// Drop or buffer overflow logic can be added
	}
}

func (t *Topic) broadcast() {
	for msg := range t.Messages {
		t.mu.RLock()
		for _, sub := range t.Subscribers {
			select {
			case sub.Ch <- msg:
			default:
				// Drop message if subscriber is too slow
			}
		}
		t.mu.RUnlock()
	}
}
