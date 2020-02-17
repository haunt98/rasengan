package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/go-redis/redis/v7"
	"github.com/gorilla/mux"
	"github.com/rs/xid"
)

const (
	successful = 1
	processing = 2
)

type ImageRequest struct {
	URL string `json:"url"`
}

type ImageResponse struct {
	UUID string `json:"uuid"`
}

type ImageStatusResponse struct {
	Status int `json:"status"`
	// URL exist only if Status is Successful
	URL string `json:"url"`
}

type imageStatusRepository struct {
	redisClient *redis.Client
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
	key := fmt.Sprintf("ai:image:uuid:%s:", guid.String())
	if err := r.redisClient.Set(key, processing, 0).Err(); err != nil {
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
	}
}

func (r *imageStatusRepository) getImageStatus(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)

	// Get image status from cache
	key := fmt.Sprintf("ai:image:uuid:%s:", vars["uuid"])
	status, err := r.redisClient.Get(key).Int()
	if err != nil {
		if err == redis.Nil {
			log.Printf("key %s not exist\n", key)
			return
		}

		log.Println(err)
		return
	}

	// Send image status response to client
	imgStatusRsp := ImageStatusResponse{
		Status: status,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(&imgStatusRsp); err != nil {
		log.Println(err)
	}
}
