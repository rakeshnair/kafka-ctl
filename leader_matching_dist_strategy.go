package kafkactl

import "errors"

// LeaderMatchingDistStrategy creates a partition assignment for a group of at least 2 distinct topics
// For the first topic in the list(Leader), it applies the Uniform Distribution Strategy.
// For all the subsequent topics(Followers) in the list it ensures that the partition distribution is the same
// as the one for the first topic
type LeaderMatchingDistStrategy struct{}

// NewMatchingDistStrategy creates a new instance of LeaderMatchingDistStrategy
func NewMatchingDistStrategy() *LeaderMatchingDistStrategy { return &LeaderMatchingDistStrategy{} }

// ErrInsufficientTopics is thrown while trying to rebalance a partition set consisting of less than 2 unique topics
var ErrInsufficientTopics = errors.New("matching distribution strategy requires partitions from at least 2 topics")

// Assignments returns partition assignments based on matching distribution strategy
func (lmds *LeaderMatchingDistStrategy) Assignments(configs StrategyConfigs) ([]PartitionReplicas, error) {
	if len(configs.TopicPartitions) == 0 {
		return []PartitionReplicas{}, ErrEmptyPartitions
	}

	brokers := configs.Brokers
	firstTopic := configs.TopicPartitions[0].Topic

	groupedTps := groupByTopic(configs.TopicPartitions)
	if len(groupedTps) < 2 {
		return []PartitionReplicas{}, ErrInsufficientTopics
	}

	uds := NewUniformDistStrategy()
	basePrs, err := uds.Assignments(StrategyConfigs{
		TopicPartitions: groupedTps[firstTopic],
		Brokers:         brokers,
	})

	if err != nil {
		return []PartitionReplicas{}, err
	}

	basePrsMap := map[topicPartition][]BrokerID{}
	for _, prs := range basePrs {
		basePrsMap[topicPartition{prs.Topic, prs.Partition}] = prs.Replicas
	}

	var finalPrs []PartitionReplicas
	finalPrs = append(finalPrs, basePrs...)

	firstTopicPartCount := int64(len(basePrsMap))

	for topic, partitions := range groupedTps {
		if topic == firstTopic {
			continue
		}
		for _, partition := range partitions {
			finalPrs = append(finalPrs, PartitionReplicas{
				Topic:     partition.Topic,
				Partition: partition.Partition,
				Replicas:  basePrsMap[topicPartition{Topic: firstTopic, Partition: partition.Partition % firstTopicPartCount}],
			})
		}
	}

	return finalPrs, nil
}

// Name returns the name of the strategy
func (lmds *LeaderMatchingDistStrategy) Name() string {
	return "leader-matching-dist-strategy"
}
