package start

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"time"
)

var ctx = context.Background()

func ExampleNewClient() {
	URL := "redis://localhost:6379/0"
	//rdb := redis.NewClient(&redis.Options{
	//	Addr:     "localhost:6379",
	//	Password: "", // no password set
	//	DB:       0,  // use default DB
	//})
	opt, err := redis.ParseURL(URL)
	if err != nil {
		fmt.Println("BAD C")
	}
	rdb := redis.NewClient(opt)
	pong, err := rdb.Ping(ctx).Result()
	fmt.Println(pong, err)
	// Output: PONG <nil>
}

func ExampleClient() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	err := rdb.Set(ctx, "key", "value", 0).Err()
	if err != nil {
		panic(err)
	}

	val, err := rdb.Get(ctx, "key").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("key", val)

	val2, err := rdb.Get(ctx, "key2").Result()
	if err == redis.Nil {
		fmt.Println("key2 does not exist")
	} else if err != nil {
		panic(err)
	} else {
		fmt.Println("key2", val2)
	}
	// Output: key value
	// key2 does not exist
}

func ExamplePubSub() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	pubsub := rdb.Subscribe(ctx)
	fmt.Println("OK")
	// Wait for confirmation that subscription is created before publishing anything.
	_, err := pubsub.Receive(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println("1")
	// Go channel which receives messages.
	ch := pubsub.Channel()

	// Publish a message.
	err = rdb.Publish(ctx, "mychannel1", "hello").Err()
	if err != nil {
		panic(err)
	}
	fmt.Println("2")

	time.AfterFunc(time.Second, func() {
		// When pubsub is closed channel is closed too.
		_ = pubsub.Close()
	})

	// Consume messages.
	for msg := range ch {
		fmt.Println(msg.Channel, msg.Payload)
	}
	fmt.Println("bad")
}

func Connect() error {
	URL := "redis://localhost:6379/0"

	opt, err := redis.ParseURL(URL)
	if err != nil {
		return err
	}
	client := redis.NewClient(opt)
	pubSub := client.Subscribe(ctx)
	err = pubSub.Ping(ctx)
	fmt.Print(err)
	return err
}