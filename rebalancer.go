package kafkactl

import "github.com/pkg/errors"

type Rebalancer struct {
	cluster    ClusterAPI
	strategies map[string]Strategy
}

var strategies = []Strategy{NewMatchingDistStrategy(), NewUniformDistStrategy()}

func NewRebalancer() *Rebalancer {
	r := Rebalancer{}

	for _, s := range strategies {
		r.strategies[s.Name()] = s
	}
	return &r
}

type RebalanceInput struct {
	Topics   []string   `json:"topics"`
	Brokers  []BrokerID `json:"brokers"`
	Strategy string     `json:"strategy"`
}

func (r *Rebalancer) Generate(input RebalanceInput) ([]PartitionDistribution, error) {
	var strategy Strategy
	strategy, exists := r.strategies[input.Strategy]
	if !exists {
		return []PartitionDistribution{}, errors.New("unknown strategy")
	}

	var tpinfos []TopicPartitionInfo
	for _, t := range input.Topics {
		tpinfo, err := r.cluster.DescribeTopic(t)
		if err != nil {
			return []PartitionDistribution{}, err
		}
		tpinfos = append(tpinfos, tpinfo...)
	}

	brokers, err := r.brokers(input.Brokers)
	if err != nil {
		return []PartitionDistribution{}, err
	}

	assignments, err := strategy.Assignments(StrategyConfigs{
		TopicPartitions: tpinfos,
		Brokers:         brokers,
	})

	return assignments, err
}

func (r *Rebalancer) Execute(assignments []PartitionDistribution) error {
	return r.cluster.ReassignPartitions(r.cluster.PartitionReassignRequest(assignments))
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
