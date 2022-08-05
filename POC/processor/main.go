package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Result struct {
	OrderNumber string
	Success     bool
}

var kafkaHost string = os.Getenv("KAFKA_HOST")
var consumerGroup string = os.Getenv("CONSUMER_GROUP")
var ingestionType string = os.Getenv("INJESTION_TYPE")
var invoiceToProcessTopic string = os.Getenv("INVOICE_TO_PROCESS_TOPIC")
var invoiceProcessedTopic string = os.Getenv("INVOICE_PROCESSED_TOPIC")

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaHost,
		"group.id":          consumerGroup,
		"auto.offset.reset": ingestionType,
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{invoiceToProcessTopic, "^aRegex.*[Tt]opic"}, nil)

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			go processInvoce(string(msg.Value))
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}

func processInvoce(orderNumber string) {
	processingMessage := fmt.Sprintf("Processing %s", orderNumber)
	fmt.Println(processingMessage)
	time.Sleep(time.Second * 10)
	result := Result{
		OrderNumber: orderNumber,
		Success:     randBool(),
	}

	produceMessage(result)

	fmt.Println("Process finished")
}

func produceMessage(orderResult Result) bool {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaHost})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed")
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := invoiceProcessedTopic

	serializedMessage, err := json.Marshal(orderResult)
	if err != nil {
		fmt.Println(err)
	}

	if e := p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          serializedMessage,
	}, nil); e != nil {
		return false
	}

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)

	return true
}

func randBool() bool {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(2) == 1
}
