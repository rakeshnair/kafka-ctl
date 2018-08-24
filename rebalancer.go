package kafkactl

import (
	"errors"
	"reflect"
)

// ErrUnknownStrategy is thrown when we try to rebalance using an unknown strategy name
var ErrUnknownStrategy = errors.New("unknown strategy")

// RebalancerConfigs wraps the configs required to instantiate a new Rebalancer instance
type RebalancerConfigs struct {
	Cluster    ClusterAPI
	Strategies []Strategy
}

// Rebalancer is responsible for executing a rebalance strategy on a given cluster
type Rebalancer struct {
	cluster    ClusterAPI
	strategies map[string]Strategy
}

// NewRebalancer returns a new instance of Rebalancer
func NewRebalancer(configs RebalancerConfigs) *Rebalancer {
	r := Rebalancer{
		cluster: configs.Cluster,
	}

	r.strategies = make(map[string]Strategy)

	for _, s := range configs.Strategies {
		r.strategies[s.Name()] = s
	}
	return &r
}

// RebalanceInput wraps the necessary information required to generate partition
// distribution plan
type RebalanceInput struct {
	Topics   []string   `json:"topics"`
	Brokers  []BrokerID `json:"brokers"`
	Strategy string     `json:"strategy"`
}

// Execute triggers a state change by updating the kafka store with the new distribution plan
func (r *Rebalancer) Execute(assignments []PartitionReplicas) error {
	return r.cluster.ReassignPartitions(r.cluster.PartitionReassignRequest(assignments))
}

// Generate accepts the rebalance input and generates a partition distribution
// Generate will automatically
// Note: The plan is just generated not applied, hence no stage change occurs here
func (r *Rebalancer) Generate(input RebalanceInput) ([]PartitionReplicas, error) {
	var strategy Strategy
	strategy, exists := r.strategies[input.Strategy]
	if !exists {
		return []PartitionReplicas{}, ErrUnknownStrategy
	}

	var currentDist []PartitionReplicas
	var tpinfos []TopicPartitionInfo
	for _, t := range input.Topics {
		tpinfo, err := r.cluster.DescribeTopic(t)
		if err != nil {
			return []PartitionReplicas{}, err
		}
		tpinfos = append(tpinfos, tpinfo...)

		pds, err := r.cluster.ReplicaDistribution(t)
		if err != nil {
			return []PartitionReplicas{}, err
		}
		currentDist = append(currentDist, pds...)
	}

	brokers, err := r.brokers(input.Brokers)
	if err != nil {
		return []PartitionReplicas{}, err
	}

	proposedDist, err := strategy.Assignments(StrategyConfigs{
		TopicPartitions: tpinfos,
		Brokers:         brokers,
	})

	return distributionDiff(proposedDist, currentDist), err
}

func (r *Rebalancer) brokers(ids []BrokerID) ([]Broker, error) {
	if len(ids) == 0 {
		return r.cluster.Brokers()
	}
	var brokers []Broker
	for _, id := range ids {
		broker, err := r.cluster.Broker(id)
		if err != nil {
			return []Broker{}, err
		}
		brokers = append(brokers, broker)
	}
	return brokers, nil
}

func distributionDiff(new []PartitionReplicas, old []PartitionReplicas) []PartitionReplicas {
	currentDistMap := map[topicPartition][]BrokerID{}

	var diff []PartitionReplicas
	for _, pd := range old {
		currentDistMap[topicPartition{pd.Topic, pd.Partition}] = pd.Replicas
	}
	for _, pd := range new {
		tp := topicPartition{pd.Topic, pd.Partition}

		var currentReplicas []BrokerID
		currentReplicas, exists := currentDistMap[tp]
		if !exists {
			diff = append(diff, pd)
			continue
		}
		if !reflect.DeepEqual(currentReplicas, pd.Replicas) {
			diff = append(diff, pd)
		}
	}
	return diff
}
