package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-redis/redis/v7"
	"github.com/gorilla/mux"
	"github.com/rs/xid"
)

const (
	processing = 2
)

type ImageRequest struct {
	URL string `json:"url"`
}

type ImageResponse struct {
	UUID string `json:"uuid"`
}

type ImageStatus struct {
	Status       int    `json:"status"`
	OriginalURL  string `json:"original_url"`
	ProcessedURL string `json:"processed_url"`
}

type ImageProcessMessage struct {
	UUID string `json:"uuid"`
	URL  string `json:"url"`
}

type imageStatusRepository struct {
	redisClient   *redis.Client
	kafkaProducer *kafka.Producer
}

func (r *imageStatusRepository) receiveImage(w http.ResponseWriter, req *http.Request) {
	var imgReq ImageRequest
	if err := json.NewDecoder(req.Body).Decode(&imgReq); err != nil {
		log.Println(err)
		return
	}

	// Generate UUID
	guid := xid.New()

	// Save image status to cache
	status, err := json.Marshal(&ImageStatus{
		Status:      processing,
		OriginalURL: imgReq.URL,
	})
	if err != nil {
		log.Println(err)
		return
	}

	key := r.genImageStatusKey(guid.String())
	if err := r.redisClient.Set(key, status, 0).Err(); err != nil {
		log.Println(err)
		return
	}

	// Send message to kafka
	imgProcessMsg := ImageProcessMessage{
		UUID: guid.String(),
		URL:  imgReq.URL,
	}

	if err := r.sendImageProcessMessage(imgProcessMsg); err != nil {
		log.Println(err)
		return
	}

	// Send UUID response to client
	imgRsp := ImageResponse{
		UUID: guid.String(),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(&imgRsp); err != nil {
		log.Println(err)
		return
	}

	log.Println("LGTM")
}

func (r *imageStatusRepository) getImageStatus(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)

	// Get image status from cache
	key := r.genImageStatusKey(vars["uuid"])
	result, err := r.redisClient.Get(key).Result()
	if err != nil {
		if err == redis.Nil {
			log.Printf("key %s not exist\n", key)
			return
		}

		log.Println(err)
		return
	}

	var imgStatus ImageStatus
	if err := json.Unmarshal([]byte(result), &imgStatus); err != nil {
		log.Println(err)
		return
	}

	// Send image status response to client
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(&imgStatus); err != nil {
		log.Println(err)
		return
	}

	log.Println("LGTM")
}

func (r *imageStatusRepository) sendImageProcessMessage(imgProcessMsg ImageProcessMessage) error {
	msg, err := json.Marshal(&imgProcessMsg)
	if err != nil {
		return err
	}

	topic := "image_process"
	if err := r.kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: msg,
	}, nil); err != nil {
		return err
	}

	return nil
}

func (r *imageStatusRepository) genImageStatusKey(uuid string) string {
	return fmt.Sprintf("image:uuid:%s", uuid)
}
