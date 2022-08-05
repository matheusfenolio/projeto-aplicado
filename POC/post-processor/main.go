package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type Result struct {
	OrderNumber string
	Success     bool
}

type Order struct {
	gorm.Model
	Id        string
	Value     float64
	Processed bool
	Success   bool
}

var kafkaHost string = os.Getenv("KAFKA_HOST")
var consumerGroup string = os.Getenv("CONSUMER_GROUP")
var ingestionType string = os.Getenv("INJESTION_TYPE")
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

	c.SubscribeTopics([]string{invoiceProcessedTopic, "^aRegex.*[Tt]opic"}, nil)

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			orderStatus := Result{}
			json.Unmarshal(msg.Value, &orderStatus)

			processOrderStatus(orderStatus)
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}

func processOrderStatus(orderStatus Result) {
	database := getDatabaseConnection()

	var order Order
	err := database.First(&order, orderStatus.OrderNumber)

	if errors.Is(err.Error, gorm.ErrRecordNotFound) {
		msg := fmt.Sprintf("Order %s  not found!", orderStatus.OrderNumber)
		fmt.Println(msg)
		return
	}

	database.Model(&order).Updates(Order{Processed: true, Success: orderStatus.Success})
}

func getDatabaseConnection() *gorm.DB {
	db, err := gorm.Open(sqlite.Open("./database/comercial.db"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		panic("failed to connect database")
	}

	// Migrate the schema
	db.AutoMigrate(&Order{})

	db.Create(&Order{Id: "123", Value: 123.00, Processed: false, Success: false})
	// db.Create(&Order{Id: "1", Value: 1.00, Processed: false, Success: false})

	return db
}
