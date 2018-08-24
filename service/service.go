package service

import (
	"net/http"

	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-ctl"
)

type ControlServiceConfigs struct {
	Clusters map[string]kafkactl.ClusterAPI
}

type ControlService struct {
	clusters map[string]kafkactl.ClusterAPI
}

func NewControlService(cfg ControlServiceConfigs) *ControlService {
	return &ControlService{
		clusters: cfg.Clusters,
	}
}

var ErrUnknownCluster = errors.New("unknown cluster")

func (s *ControlService) Ping(r *http.Request, req *kafkactl.PingRequest, res *kafkactl.PingResponse) (err error) {
	res.Seed = req.Seed
	res.Time = time.Now()
	return err
}

func (s *ControlService) ClusterID(r *http.Request, req *kafkactl.ClusterIDRequest, res *kafkactl.ClusterIDResponse) (err error) {
	cluster, exists := s.clusters[req.Name]
	if !exists {
		return ErrUnknownCluster
	}
	res.ID, err = cluster.ID()
	return err
}

func (s *ControlService) ClusterController(r *http.Request, req *kafkactl.ClusterControllerRequest, res *kafkactl.ClusterControllerResponse) (err error) {
	cluster, exists := s.clusters[req.Name]
	if !exists {
		return ErrUnknownCluster
	}
	res.Controller, err = cluster.Controller()
	return err
}

func (s *ControlService) ClusterBrokers(r *http.Request, req *kafkactl.ClusterBrokersRequest, res *kafkactl.ClusterBrokersResponse) (err error) {
	cluster, exists := s.clusters[req.Name]
	if !exists {
		return ErrUnknownCluster
	}
	res.Brokers, err = cluster.Brokers()
	return err
}

func (s *ControlService) ClusterBroker(r *http.Request, req *kafkactl.ClusterBrokerRequest, res *kafkactl.ClusterBrokerResponse) (err error) {
	cluster, exists := s.clusters[req.Name]
	if !exists {
		return ErrUnknownCluster
	}
	res.Broker, err = cluster.Broker(req.ID)
	return err
}

func (s *ControlService) ClusterTopics(r *http.Request, req *kafkactl.ClusterTopicsRequest, res *kafkactl.ClusterTopicsResponse) (err error) {
	cluster, exists := s.clusters[req.Name]
	if !exists {
		return ErrUnknownCluster
	}
	res.Topics, err = cluster.Topics()
	return err
}

func (s *ControlService) ClusterDescribeTopics(r *http.Request, req *kafkactl.ClusterDescribeTopicRequest, res *kafkactl.ClusterDescribeTopicResponse) (err error) {
	cluster, exists := s.clusters[req.Name]
	if !exists {
		return ErrUnknownCluster
	}
	res.Partitions, err = cluster.DescribeTopic(req.Topic)
	return err
}

func (s *ControlService) ClusterDescribeTopicsForBroker(r *http.Request, req *kafkactl.ClusterDescribeTopicsForBrokerRequest, res *kafkactl.ClusterDescribeTopicsForBrokerResponse) (err error) {
	cluster, exists := s.clusters[req.Name]
	if !exists {
		return ErrUnknownCluster
	}
	res.Partitions, err = cluster.DescribeTopicsForBroker(req.BrokerID)
	return err
}

func (s *ControlService) ClusterReplicaDistribution(r *http.Request, req *kafkactl.ClusterReplicaDistributionRequest, res *kafkactl.ClusterReplicaDistributionResponse) (err error) {
	cluster, exists := s.clusters[req.Name]
	if !exists {
		return ErrUnknownCluster
	}
	res.Replicas, err = cluster.ReplicaDistribution(req.Topic)
	return err
}
