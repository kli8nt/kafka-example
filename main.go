package main

import (
	"fmt"
	"os"
	"time"

	"github.com/joho/godotenv"
)

func sleep() {
	var forever chan struct{}
	<-forever
}

func main() {
	godotenv.Load()
	kafkaPort := os.Getenv("KAFKA_PORT")
	kafkaHost := os.Getenv("KAFKA_HOST")

	fmt.Println("Connecting to Kafka...")
	fmt.Printf("Host: %s\n", kafkaHost)
	fmt.Printf("Port: %s\n", kafkaPort)

	config := KConfig{
		host:  kafkaHost,
		port:  kafkaPort,
		deadline: 10,
		topic: "log-streaming",
	}

	kf := Kf{}
	kf.Init(config)
	defer kf.Close()
	reader := kf.Reader()
	writer := kf.Writer()

	go func() {
		reader.Read(func(key string, msg []byte) {
			fmt.Println("received ->", key, ":", string(msg))
		})
	}()

	everyFiveSeconds := time.NewTicker(2 * time.Second)
	defer everyFiveSeconds.Stop()

	go func() {
		i := 0
		for {
			select {
				case <-everyFiveSeconds.C:
					body := "HEYOO! person number " + fmt.Sprintf("%d", i)
					println("Writing to Kafka: " + body)
					writer.Write(fmt.Sprintf("%d", i), []byte(body))
					i++
					if i == 10 {
						i = 0
					}
				}
		}
	}()

	sleep()
}