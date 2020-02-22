package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/go-redis/redis/v7"
	"github.com/gorilla/mux"
)

func main() {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	kafkaProducer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		log.Println(err)
		return
	}
	defer kafkaProducer.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range kafkaProducer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	imageStatusRepo := imageStatusRepository{
		redisClient:   redisClient,
		kafkaProducer: kafkaProducer,
	}

	r := mux.NewRouter()
	r.HandleFunc("/ping", ping)
	r.HandleFunc("/image", imageStatusRepo.receiveImage).Methods("POST")
	r.HandleFunc("/image/{uuid}", imageStatusRepo.getImageStatus).Methods("GET")

	log.Fatal(http.ListenAndServe(":42069", r))
}

func ping(w http.ResponseWriter, _ *http.Request) {
	if _, err := fmt.Fprintf(w, "ping"); err != nil {
		log.Println(err)
		return
	}

	log.Println("LGTM")
}
