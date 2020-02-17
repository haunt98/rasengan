package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/go-redis/redis/v7"
	"github.com/gorilla/mux"
)

func main() {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	imageStatusRepo := imageStatusRepository{
		redisClient: redisClient,
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
	}
}
