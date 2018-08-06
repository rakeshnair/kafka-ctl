package main

import (
	"os"
	"path"
	"time"

	"context"

	"syscall"

	"strings"

	"github.com/Shopify/sarama"
	"github.com/segmentio/conf"
	"github.com/segmentio/events"
	_ "github.com/segmentio/events/ecslogs"
	_ "github.com/segmentio/events/text"
)

type configs struct {
	Bootstrap string `conf:"bootstrap" help:"comma separated list of kafka cluster endpoints"`
	Topic     string `conf:"topic" help:"kafka topic name"`
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
	scfg.Producer.Return.Successes = true

	_, err := sarama.NewSyncProducer(strings.Split(cfg.Bootstrap, ","), scfg)
	if err != nil {
		events.Log("failed to create new producer %{panic}v", err)
		os.Exit(1)
	}
}
