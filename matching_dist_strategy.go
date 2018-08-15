package kafkactl

import "errors"

// MatchingDistStrategy creates a partition assignment for a group of at least 2 distinct topics
// For the first topic in the list, it applies the Uniform Distribution Strategy.
// For all the subsequent topics in the list it ensures that the partition distribution is the same
// as the one for the first topic
type MatchingDistStrategy struct{}

// NewMatchingDistStrategy creates a new instance of MatchingDistStrategy
func NewMatchingDistStrategy() *MatchingDistStrategy { return &MatchingDistStrategy{} }

// ErrInsufficientTopics is thrown while trying to rebalance a partition set consisting of less than 2 unique topics
var ErrInsufficientTopics = errors.New("matching distribution strategy requires partitions from at least 2 topics")

// Assignments returns partition assignments based on matching distribution strategy
func (mds *MatchingDistStrategy) Assignments(configs StrategyConfigs) ([]PartitionDistribution, error) {
	if len(configs.TopicPartitions) == 0 {
		return []PartitionDistribution{}, ErrEmptyPartitions
	}

	brokers := configs.Brokers
	firstTopic := configs.TopicPartitions[0].Topic

	groupedTps := groupByTopic(configs.TopicPartitions)
	if len(groupedTps) < 2 {
		return []PartitionDistribution{}, ErrInsufficientTopics
	}

	uds := NewUniformDistStrategy()
	basePrs, err := uds.Assignments(StrategyConfigs{
		TopicPartitions: groupedTps[firstTopic],
		Brokers:         brokers,
	})

	if err != nil {
		return []PartitionDistribution{}, err
	}

	basePrsMap := map[topicPartition][]BrokerID{}
	for _, prs := range basePrs {
		basePrsMap[topicPartition{prs.Topic, prs.Partition}] = prs.Replicas
	}

	var finalPrs []PartitionDistribution
	finalPrs = append(finalPrs, basePrs...)

	firstTopicPartCount := int64(len(basePrsMap))

	for topic, partitions := range groupedTps {
		if topic == firstTopic {
			continue
		}
		for _, partition := range partitions {
			finalPrs = append(finalPrs, PartitionDistribution{
				Topic:     partition.Topic,
				Partition: partition.Partition,
				Replicas:  basePrsMap[topicPartition{Topic: firstTopic, Partition: partition.Partition % firstTopicPartCount}],
			})
		}
	}

	return finalPrs, nil
}
