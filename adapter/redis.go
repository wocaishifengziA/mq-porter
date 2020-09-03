package adapter

import (
	"fmt"
	"github.com/go-redis/redis"
	"log"
	"sync"
)

type RedisMessageClient struct {
	URI     string
	redis   *redis.Client
	pubsub  *redis.PubSub
	channel chan *Message
	mu      *sync.RWMutex
}

func (c *RedisMessageClient) String() string {
	return fmt.Sprintf("Redis URI: %s", c.URI)
}

func (c *RedisMessageClient) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	opt, err := redis.ParseURL(c.URI)
	if err != nil {
		return err
	}
	client := redis.NewClient(opt)
	c.redis = client
	c.pubsub = client.Subscribe()
	err = c.pubsub.Ping()
	return err
}

// Close bala
func (c *RedisMessageClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var err error
	if c.pubsub != nil {
		err = c.pubsub.Close()
		c.pubsub = nil
		if c.channel != nil {
			// c.channel <- nil
			close(c.channel)
			c.channel = nil
		}
	}
	if c.redis != nil {
		err = c.redis.Close()
		c.redis = nil
	}
	return err
}

// IsClosed 判断当前连接是否断开
func (c *RedisMessageClient) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.redis != nil {
		r := c.redis.Ping()
		if r != nil {
			return r.Err() != nil
		}
	}
	return true
}

func (c *RedisMessageClient) getPubsub() *redis.PubSub {
	if c.pubsub == nil {
		for {
			if err := c.Connect(); err == nil {
				break
			}
		}
	}
	return c.pubsub
}

func (c *RedisMessageClient) getRedis() *redis.Client {
	if c.redis == nil {
		for {
			if err := c.Connect(); err == nil {
				break
			}
		}
	}
	return c.redis
}

// Subscribe
func (c *RedisMessageClient) Subscribe(topics ...string) error {
	return c.getPubsub().Subscribe(topics...)
}

// Unsubscribe bala
func (c *RedisMessageClient) Unsubscribe(topics ...string) error {
	return c.getPubsub().Unsubscribe(topics...)
}

// AllTopics bala
func (c *RedisMessageClient) AllTopics() (topics []string, err error) {
	result := c.getRedis().PubSubChannels("*")

	if result == nil || result.Err() != nil {
		return nil, result.Err()
	}
	return result.Result()
}

// GetChan bala
func (c *RedisMessageClient) GetChan() <-chan *Message {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.channel == nil {
		log.Println("Make Chan:", c.URI)
		c.channel = make(chan *Message)
		go func() {
			var msg *redis.Message
			channel := c.getPubsub().Channel()
			for {
				msg = <-channel
				c.channel <- &Message{topic: msg.Channel, data: msg.Payload}
			}
		}()
	}

	return c.channel
}

// Publish bala
func (c *RedisMessageClient) Publish(topic string, message interface{}) error {
	result := c.getRedis().Publish(topic, message)
	if result != nil {
		return result.Err()
	}
	return nil
}
