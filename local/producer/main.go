package main

import (
	"os"
	"path"
	"time"

	"context"

	"syscall"

	"strings"

	"math/rand"

	"encoding/json"

	"github.com/Shopify/sarama"
	"github.com/segmentio/conf"
	"github.com/segmentio/events"
	_ "github.com/segmentio/events/ecslogs"
	_ "github.com/segmentio/events/text"
	"github.com/segmentio/ksuid"
)

func init() {
	rand.Seed(time.Now().Unix())
}

type configs struct {
	Bootstrap string `conf:"bootstrap" help:"comma separated list of kafka cluster endpoints"`
	Topic     string `conf:"topic" help:"kafka topic name"`
	Count     int    `conf:"count" help:total messages to send to kafka`
}

var (
	version = "dev"
	types   = []string{"track", "page", "view", "identify", "alias"}
)

type Message struct {
	Id        string    `json:"id"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	prog := path.Base(os.Args[0])
	events.Log("%{program}s version: %{version}s", prog, version)

	cfg := configs{}
	conf.Load(&cfg)
	events.Log("starting producer with configs %{config}v", cfg)

	_, cancel := events.WithSignals(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	scfg := sarama.NewConfig()
	scfg.Version = sarama.V1_1_0_0
	scfg.Producer.Compression = sarama.CompressionSnappy
	scfg.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(strings.Split(cfg.Bootstrap, ","), scfg)
	if err != nil {
		events.Log("failed to create new producer %{panic}v", err)
		os.Exit(1)
	}

	for i := 0; i < cfg.Count; i++ {
		msg, err := json.Marshal(Message{
			Id:        ksuid.New().String(),
			Type:      types[rand.Intn(len(types))],
			Timestamp: time.Now().UTC(),
		})
		if err != nil {
			events.Log("failed to parse message. %{err}v", err)
			os.Exit(2)
		}

		partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: cfg.Topic,
			Value: sarama.ByteEncoder(msg)})
		if err != nil {
			events.Log("failed to send message to Kafka. %{err}v", err)
			os.Exit(2)
		}
		events.Log("> message sent to partition %d at offset %d", partition, offset)
	}
}
