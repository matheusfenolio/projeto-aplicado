package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
)

var producerTopic string = os.Getenv("INVOICE_TO_PROCESS_TOPIC")
var kafkaHost string = os.Getenv("KAFKA_HOST")

func main() {
	r := gin.Default()
	r.POST("/invoice/:orderNumber", func(c *gin.Context) {
		orderNumber := c.Param("orderNumber")

		success := produceMessage(orderNumber)

		var httpStatus int
		var message string

		if success {
			httpStatus = http.StatusOK
			message = fmt.Sprintf("Order %s was produced with success!", orderNumber)
		} else {
			httpStatus = http.StatusInternalServerError
			message = fmt.Sprintf("Order %s was not procced!", orderNumber)
		}

		c.JSON(httpStatus, gin.H{
			"message": message,
		})
	})

	r.Run() // listen and serve on 0.0.0.0:8080 (for windows "localhost:8080")
}

func getProducer() (*kafka.Producer, error) {
	fmt.Println("Trying to connect to: ", kafkaHost)

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaHost})

	if err != nil {
		return nil, err
	}

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

	return p, nil
}

func produceMessage(orderNumber string) bool {
	producer, err := getProducer()

	if err != nil {
		log.Fatal(err)
		return false
	}

	defer producer.Close()

	topic := producerTopic

	if e := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(orderNumber),
	}, nil); e != nil {
		return false
	}

	producer.Flush(15 * 1000)

	return true
}
