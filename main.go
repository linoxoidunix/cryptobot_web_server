package main

import (
	"log"
	"net/http"
	"sync"

	"github.com/IBM/sarama"
	"github.com/gorilla/websocket"
)

// Настройки WebSocket
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// WebSocket handler
func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Error upgrading connection:", err)
		return
	}
	defer conn.Close()

	// Create a channel for sending WebSocket messages
	messageChannel := make(chan []byte)

	// Start a goroutine for writing to the WebSocket
	go writeToWebSocket(conn, messageChannel)

	// Setup Kafka consumer to subscribe to "orderbook", "pnl", and "wallet" topics concurrently
	var wg sync.WaitGroup

	// Start consuming messages from the "orderbook" topic
	wg.Add(1)
	go consumeMessages("orderbook", conn, messageChannel, &wg)

	// Start consuming messages from the "pnl" topic
	wg.Add(1)
	go consumeMessages("pnl", conn, messageChannel, &wg)

	// Start consuming messages from the "wallet" topic
	wg.Add(1)
	go consumeMessages("wallet", conn, messageChannel, &wg)

	// Start consuming messages from the "wallet" topic
	wg.Add(1)
	go consumeMessages("trade", conn, messageChannel, &wg)

	// Wait for all consumers to finish
	wg.Wait()
}

// Function to consume messages from Kafka
func consumeMessages(topic string, conn *websocket.Conn, messageChannel chan []byte, wg *sync.WaitGroup) {
	defer wg.Done()

	// Setup Kafka consumer to subscribe to the given topic
	consumer := startKafkaConsumer()
	defer consumer.Close()

	// Subscribe to the topic
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Printf("Error fetching partitions for topic %s: %v\n", topic, err)
		return
	}

	// Consume messages from each partition
	for _, partition := range partitions {
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Printf("Error subscribing to partition %d for topic %s: %v\n", partition, topic, err)
			return
		}
		defer pc.Close()

		// Loop to listen for new messages
		for message := range pc.Messages() {
			// Логирование сырых данных (до обработки)
			log.Printf("Received raw Kafka message from topic %s: %s\n", topic, string(message.Value))

			// Send the raw message to the messageChannel for WebSocket writing
			messageChannel <- message.Value
		}
	}
}

// Function to handle WebSocket writes
func writeToWebSocket(conn *websocket.Conn, messageChannel chan []byte) {
	for message := range messageChannel {
		// Write the message to WebSocket connection
		err := conn.WriteMessage(websocket.TextMessage, message)
		if err != nil {
			log.Println("Error sending message over WebSocket:", err)
			return
		}
	}
}

// Kafka Consumer (подписка на топик)
func startKafkaConsumer() sarama.Consumer {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	consumer, err := sarama.NewConsumer([]string{"localhost:19092"}, config)
	if err != nil {
		log.Fatal("Error creating Kafka consumer:", err)
	}
	return consumer
}

func main() {
	// HTTP-сервер для WebSocket
	http.HandleFunc("/ws", wsHandler)

	// Запуск сервера
	log.Println("Starting server on 0.0.0.0:10999")
	if err := http.ListenAndServe("0.0.0.0:10999", nil); err != nil {
		log.Fatal("Error starting server:", err)
	}
}
