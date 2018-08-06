package main

import (
	"encoding/json"
	"os"
	"path"
	"strings"
	"syscall"

	"context"

	"strconv"

	"sync"

	"github.com/Shopify/sarama"
	"github.com/segmentio/conf"
	"github.com/segmentio/events"
	_ "github.com/segmentio/events/ecslogs"
	_ "github.com/segmentio/events/text"
	"github.com/segmentio/kafka-ctl/local"
)

type configs struct {
	Bootstrap  string `conf:"bootstrap" help:"comma separated list of kafka cluster endpoints"`
	Topic      string `conf:"topic" help:"kafka topic name"`
	Partitions string `conf:"partitions" help:"comma separate list of partitions to consume from"`
}

var (
	version = "dev"
)

func main() {
	prog := path.Base(os.Args[0])
	events.Log("%{program}s version: %{version}s", prog, version)

	ctx, cancel := events.WithSignals(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg := configs{}
	conf.Load(&cfg)
	events.Log("starting consumer with configs %{config}v", cfg)

	scfg := sarama.NewConfig()
	scfg.Version = sarama.V1_1_0_0

	consumer, err := sarama.NewConsumer(strings.Split(cfg.Bootstrap, ","), scfg)
	if err != nil {
		events.Log("failed to create new consumer %{panic}v", err)
		os.Exit(1)
	}

	var wg sync.WaitGroup
	partitions := strings.Split(cfg.Partitions, ",")
	for _, p := range partitions {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()

			pint, err := strconv.ParseInt(p, 10, 64)
			if err != nil {
				events.Log("failed to parse partition value. exiting routine. %{err}v", err)
				return
			}

			pconsumer, err := consumer.ConsumePartition(cfg.Topic, int32(pint), sarama.OffsetOldest)
			if err != nil {
				events.Log("failed to create partition consumer. exiting routine. %{err}v", err)
				return
			}

			events.Log("starting to consume from partition %{partition}d", pint)

			for {
				select {
				case mb := <-pconsumer.Messages():
					m := &local.Message{}
					err := json.Unmarshal(mb.Value, m)
					if err != nil {
						events.Log("failed to parse message. %{err}v", err)
						continue
					}
					events.Log("got message %{msg}v", *m)
				case <-ctx.Done():
					events.Log("shutting down partition %{partition}d consumer", pint)
					pconsumer.Close()
					return
				}
			}
		}(p)
	}
	wg.Wait()
	events.Log("consumer application exiting")
}
