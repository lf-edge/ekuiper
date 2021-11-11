package main

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"os"
)

func getClient(host, key string) {
	add := fmt.Sprintf("%s:6379", host)
	rdb := redis.NewClient(&redis.Options{
		Addr:     add,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	val, err := rdb.Get(key).Result()
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", val)
}

func setClient(host, key string) {
	add := fmt.Sprintf("%s:6379", host)
	rdb := redis.NewClient(&redis.Options{
		Addr:     add,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	err := rdb.Set(key, "value", 0).Err()
	if err != nil {
		panic(err)
	}

}

func main() {
	if len(os.Args) == 4 {
		if v := os.Args[1]; v == "set" {
			//The 2nd parameter is MQTT broker server address
			setClient(os.Args[2], os.Args[3])
		}
		if v := os.Args[1]; v == "get" {
			//The 2nd parameter is MQTT broker server address
			getClient(os.Args[2], os.Args[3])
		}
	}
}
