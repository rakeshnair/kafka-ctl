package kafkactl

import (
	"sort"

	"github.com/pkg/errors"
)

// UniformDistStrategyConfigs wraps the list of configs required to initialize a UniformDistStrategy object
type UniformDistStrategyConfigs struct {
	Cluster ClusterAPI `json:"cluster"`
	Topics  []string   `json:"topics"`
	Brokers []BrokerID `json:"brokerIDs"`
}

// UniformDistStrategy implements a Strategy that uniformly distributes all topic partitions among available brokers
type UniformDistStrategy struct {
	cluster   ClusterAPI
	topics    []string
	brokerIDs []BrokerID
}

// NewUniformDistStrategy returns a new instance of UniformDistStrategy
func NewUniformDistStrategy(configs UniformDistStrategyConfigs) (*UniformDistStrategy, error) {
	if len(configs.Topics) == 0 {
		return nil, ErrTopicsMissing
	}
	return &UniformDistStrategy{
		cluster:   configs.Cluster,
		topics:    configs.Topics,
		brokerIDs: configs.Brokers,
	}, nil
}

// Assignments returns a distribution where partitions are uniformly distributed among brokers
func (uds *UniformDistStrategy) Assignments() ([]PartitionReplicas, error) {
	var prs []PartitionReplicas
	for _, topic := range uds.topics {
		tprs, err := uds.topicAssignments(topic)
		if err != nil {
			return []PartitionReplicas{}, err
		}
		prs = append(prs, tprs...)
	}
	sort.Sort(byPartitionInPartitionReplicas(prs))
	return prs, nil
}

func (uds *UniformDistStrategy) brokers(ids ...BrokerID) ([]Broker, error) {
	if len(ids) == 0 {
		return uds.cluster.Brokers()
	}
	var brokers []Broker
	for _, id := range ids {
		broker, err := uds.cluster.Broker(id)
		if err != nil {
			return []Broker{}, err
		}
		brokers = append(brokers, broker)
	}
	return brokers, nil
}

func (uds *UniformDistStrategy) topicAssignments(topic string) ([]PartitionReplicas, error) {
	brokers, err := uds.brokers(uds.brokerIDs...)
	if err != nil {
		return []PartitionReplicas{}, err
	}

	isRackAware := true
	for _, broker := range brokers {
		isRackAware = isRackAware && (len(broker.Rack) > 0)
	}

	tps, err := uds.cluster.DescribeTopic(topic)
	if err != nil {
		return []PartitionReplicas{}, err
	}

	if len(tps) == 0 {
		return []PartitionReplicas{}, errors.New("no partitions to assign")
	}

	replicationFactor := tps[0].Replication

	// Set of all BrokerIDs available for serving partitions
	bidSet := buildBrokerIDSet(brokers, isRackAware)

	// pick a random broker to start assigning partitions
	startIndex := randomStartIndexFn(bidSet.Size())

	// Mapping between a specific partition and the bidSet holding a replica
	tpBrokerMap := map[TopicPartition]*BrokerIDTreeSet{}
	for _, tp := range tps {
		tpBrokerMap[tp.TopicPartition] = NewBrokerIDSet()
	}

	// partitions should be assigned to brokerIDs "replicationFactor" times
	for i := 0; i < replicationFactor; i++ {
		j := startIndex
		for _, tp := range tps {
			for {
				brokerID, err := bidSet.Get(j)
				if err != nil {
					switch err {
					case ErrSetIndexOutOfBounds:
						j = j + 1
						if j >= bidSet.Size() {
							j = 0
						}
						continue
					default:
						return []PartitionReplicas{}, err
					}

				}
				success := tpBrokerMap[tp.TopicPartition].Add(brokerID)
				if success {
					break
				}
				// pick next broker if its already associated with the partition
				j = j + 1
			}

			// move to next broker once the given partition has been associated with a broker
			j = j + 1
		}
		// move to a new broker for the next iteration
		startIndex = startIndex + 1
		if startIndex >= bidSet.Size() {
			startIndex = 0
		}
	}

	return toPartitionReplicas(tpBrokerMap), nil
}
