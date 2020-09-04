package main

import (
	"fmt"
	"porter/mq/redis/adapter"
)

func main() {
	//start.ExampleNewClient()
	//start.Connect()

	redisClient := adapter.NewRedisMessageClient("redis://172.18.19.63:6379/0")
	redisClient.Connect()
	redisClient.Subscribe("hello", "good")
	topics, _ := redisClient.AllTopics()
	fmt.Println(topics)
	chMsg := redisClient.GetChan()
	for {
		w := <- chMsg
		fmt.Println(w)
	}
}
