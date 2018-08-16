package kafkactl

import (
	"errors"
	"sort"
)

// UniformDistStrategy implements a Strategy that uniformly distributes all topic partitions among available brokers
type UniformDistStrategy struct{}

// NewUniformDistStrategy returns a new instance of UniformDistStrategy
func NewUniformDistStrategy() *UniformDistStrategy { return &UniformDistStrategy{} }

// ErrEmptyPartitions is thrown when we try to perform a rebalancing by providing an empty list of partitions
var ErrEmptyPartitions = errors.New("partition list for rebalancing is empty")

// Assignments returns a distribution where partitions are uniformly distributed among brokers
func (uds *UniformDistStrategy) Assignments(configs StrategyConfigs) ([]PartitionDistribution, error) {
	if len(configs.TopicPartitions) == 0 {
		return []PartitionDistribution{}, ErrEmptyPartitions
	}

	brokers := configs.Brokers
	groupedTps := groupByTopic(configs.TopicPartitions)

	var topics []string
	for topic, _ := range groupedTps {
		topics = append(topics, topic)
	}

	sort.Strings(topics)

	var prs []PartitionDistribution
	for _, topic := range topics {
		tprs, err := uds.generateAssignments(topic, brokers, groupedTps[topic])
		if err != nil {
			return []PartitionDistribution{}, err
		}
		prs = append(prs, tprs...)
	}

	return prs, nil
}

// Name returns the name of the strategy
func (uds *UniformDistStrategy) Name() string {
	return "uniform-dist-strategy"
}

func (uds *UniformDistStrategy) generateAssignments(topic string, brokers []Broker, partitions []TopicPartitionInfo) ([]PartitionDistribution, error) {
	// Check if rack awareness need to be considered
	isRackAware := true
	for _, broker := range brokers {
		isRackAware = isRackAware && (len(broker.Rack) > 0)
	}

	replicationFactor := partitions[0].Replication

	// Set of all BrokerIDs available for serving partitions
	bidSet := buildBrokerIDSet(brokers, isRackAware)

	// pick a broker based on the existing allocation
	startBroker := partitions[0].Leader

	startIndex, err := bidSet.IndexOf(startBroker)
	if err != nil {
		return []PartitionDistribution{}, err
	}

	// Mapping between a specific partition and the bidSet holding a replica
	tpBrokerMap := map[topicPartition]*BrokerIDTreeSet{}
	for _, tp := range partitions {
		tpBrokerMap[topicPartition{tp.Topic, tp.Partition}] = NewBrokerIDSet()
	}

	// partitions should be assigned to brokerIDs "replicationFactor" times
	for i := 0; i < replicationFactor; i++ {
		j := startIndex
		for _, tp := range partitions {
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
						return []PartitionDistribution{}, err
					}

				}
				success := tpBrokerMap[topicPartition{tp.Topic, tp.Partition}].Add(brokerID)
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

	return generatePartitionDistribution(tpBrokerMap), nil
}
