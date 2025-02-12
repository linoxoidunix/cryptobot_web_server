package kafka

import (
	"log"
	"sync"

	"cryptobot_server/handlers"

	"github.com/IBM/sarama"
)

func startKafkaConsumer() sarama.Consumer {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	consumer, err := sarama.NewConsumer([]string{"localhost:19092"}, config)
	if err != nil {
		log.Fatal("Error creating Kafka consumer:", err)
	}
	return consumer
}

func ConsumeMessages(topic string, messageChannel chan []byte, wg *sync.WaitGroup) {
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
			// Log raw Kafka message data (before processing)
			log.Printf("Received raw Kafka message from topic %s: %s\n", topic, string(message.Value))

			// Pass the raw message and message channel to the appropriate handler
			if handler, exists := handlers.TopicHandlers[topic]; exists {
				handler(messageChannel, message.Value)
			} else {
				log.Printf("No handler defined for topic: %s", topic)
			}
		}
	}
}
