package main

import (
	"context"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
)

func getClient(host, key string) {
	ctx := context.Background()
	add := fmt.Sprintf("%s:6379", host)
	rdb := redis.NewClient(&redis.Options{
		Addr:     add,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	val, err := rdb.Get(ctx, key).Result()
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", val)
}

func setClient(host, key string) {
	ctx := context.Background()
	add := fmt.Sprintf("%s:6379", host)
	rdb := redis.NewClient(&redis.Options{
		Addr:     add,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	err := rdb.Set(ctx, key, "value", 0).Err()
	if err != nil {
		panic(err)
	}
}

func main() {
	if len(os.Args) == 4 {
		if v := os.Args[1]; v == "set" {
			// The 2nd parameter is MQTT broker server address
			setClient(os.Args[2], os.Args[3])
		}
		if v := os.Args[1]; v == "get" {
			// The 2nd parameter is MQTT broker server address
			getClient(os.Args[2], os.Args[3])
		}
	}
}
