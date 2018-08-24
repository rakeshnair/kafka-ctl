package main

import (
	"net/http"

	"os"
	"path"

	"context"
	"syscall"

	"time"

	"github.com/gorilla/rpc/v2"
	"github.com/gorilla/rpc/v2/json"
	"github.com/segmentio/conf"
	"github.com/segmentio/events"
	_ "github.com/segmentio/events/ecslogs"
	_ "github.com/segmentio/events/text"
	"github.com/segmentio/kafka-ctl"
	"github.com/segmentio/kafka-ctl/service"
)

type configs struct {
	Broker    string `conf:"broker" help:"name of the kafka cluster"`
	Zookeeper string `conf:"zookeeper" help:"zookeeper endpoint for the cluster"`
	Addr      string `conf:"addr" help:"endpoint to start a server"`
}

var (
	version = "dev"
)

func main() {
	prog := path.Base(os.Args[0])
	events.Log("%{program}s version: %{version}s", prog, version)

	cfg := configs{Addr: "localhost:8888"}
	conf.Load(&cfg)
	events.Log("starting server with configs %{config}v", cfg)

	ctx, cancel := events.WithSignals(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	store, err := kafkactl.NewZkClusterStore(cfg.Zookeeper, time.Second*10)
	if err != nil {
		events.Log("failed to create a new zookeeper cluster store", events.Args{
			{"endpoint", cfg.Zookeeper},
			{"error", err},
		})
		os.Exit(2)
	}

	svc := service.NewControlService(service.ControlServiceConfigs{
		Clusters: map[string]kafkactl.ClusterAPI{cfg.Broker: kafkactl.NewCluster(store)},
	})

	s := rpc.NewServer()
	s.RegisterCodec(json.NewCodec(), "application/json")
	s.RegisterService(svc, "KafkaControl")
	http.Handle("/rpc", s)
	server := &http.Server{Addr: cfg.Addr, Handler: nil}

	httpDone := make(chan struct{})
	go runserver(ctx, server, httpDone)

	<-ctx.Done()
	events.Log("shutdown detected... waiting for http server shutdown")
	<-httpDone
	events.Log("http server shutdown")
}

func runserver(ctx context.Context, server *http.Server, done chan struct{}) {
	defer close(done)
	go server.ListenAndServe()
	<-ctx.Done()

	// gracefully shutdown http server
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	server.Shutdown(ctx)
}
