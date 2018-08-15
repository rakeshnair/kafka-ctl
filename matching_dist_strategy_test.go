package kafkactl

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMatchingDistStrategy_Assignments(t *testing.T) {
	t.Run("two_topics_eql_partitions", func(t *testing.T) {
		// --input
		configs := StrategyConfigs{
			Brokers: []Broker{{Id: 1, Rack: "1"}, {Id: 2, Rack: "1"}, {Id: 3, Rack: "2"}},
			TopicPartitions: []TopicPartitionInfo{
				{Topic: "kafka-test-1", Partition: 0, Replication: 2, Leader: 3},
				{Topic: "kafka-test-1", Partition: 1, Replication: 2, Leader: 2},
				{Topic: "kafka-test-1", Partition: 2, Replication: 2, Leader: 1},
				{Topic: "kafka-test-2", Partition: 0, Replication: 2, Leader: 3},
				{Topic: "kafka-test-2", Partition: 1, Replication: 2, Leader: 3},
				{Topic: "kafka-test-2", Partition: 2, Replication: 2, Leader: 3},
			},
		}

		// --expected
		expected := []PartitionDistribution{
			{"kafka-test-1", 0, []BrokerID{3, 2}},
			{"kafka-test-1", 1, []BrokerID{2, 1}},
			{"kafka-test-1", 2, []BrokerID{1, 3}},
			{"kafka-test-2", 0, []BrokerID{3, 2}},
			{"kafka-test-2", 1, []BrokerID{2, 1}},
			{"kafka-test-2", 2, []BrokerID{1, 3}},
		}

		cluster := NewMatchingDistStrategy()
		actual, err := cluster.Assignments(configs)
		if assert.NoError(t, err) {
			sort.Sort(byPartitionInPartitionReplicas(expected))
			sort.Sort(byPartitionInPartitionReplicas(actual))
			assert.EqualValues(t, expected, actual)
		}
	})

	t.Run("two_topics_uneql_partitions_second_less", func(t *testing.T) {
		// --input
		configs := StrategyConfigs{
			Brokers: []Broker{{Id: 1, Rack: "1"}, {Id: 2, Rack: "1"}, {Id: 3, Rack: "2"}},
			TopicPartitions: []TopicPartitionInfo{
				{Topic: "kafka-test-1", Partition: 0, Replication: 2, Leader: 3},
				{Topic: "kafka-test-1", Partition: 1, Replication: 2, Leader: 2},
				{Topic: "kafka-test-1", Partition: 2, Replication: 2, Leader: 1},
				{Topic: "kafka-test-2", Partition: 0, Replication: 2, Leader: 3},
				{Topic: "kafka-test-2", Partition: 1, Replication: 2, Leader: 3},
			},
		}

		// --expected
		expected := []PartitionDistribution{
			{"kafka-test-1", 0, []BrokerID{3, 2}},
			{"kafka-test-1", 1, []BrokerID{2, 1}},
			{"kafka-test-1", 2, []BrokerID{1, 3}},
			{"kafka-test-2", 0, []BrokerID{3, 2}},
			{"kafka-test-2", 1, []BrokerID{2, 1}},
		}

		cluster := NewMatchingDistStrategy()
		actual, err := cluster.Assignments(configs)
		if assert.NoError(t, err) {
			sort.Sort(byPartitionInPartitionReplicas(expected))
			sort.Sort(byPartitionInPartitionReplicas(actual))
			assert.EqualValues(t, expected, actual)
		}
	})

	t.Run("two_topics_uneql_partitions_second_more", func(t *testing.T) {
		// --input
		configs := StrategyConfigs{
			Brokers: []Broker{{Id: 1, Rack: "1"}, {Id: 2, Rack: "1"}, {Id: 3, Rack: "2"}},
			TopicPartitions: []TopicPartitionInfo{
				{Topic: "kafka-test-1", Partition: 0, Replication: 2, Leader: 3},
				{Topic: "kafka-test-1", Partition: 1, Replication: 2, Leader: 2},
				{Topic: "kafka-test-2", Partition: 0, Replication: 2, Leader: 3},
				{Topic: "kafka-test-2", Partition: 1, Replication: 2, Leader: 3},
				{Topic: "kafka-test-2", Partition: 2, Replication: 2, Leader: 1},
			},
		}

		// --expected
		expected := []PartitionDistribution{
			{"kafka-test-1", 0, []BrokerID{3, 2}},
			{"kafka-test-1", 1, []BrokerID{2, 1}},
			{"kafka-test-2", 0, []BrokerID{3, 2}},
			{"kafka-test-2", 1, []BrokerID{2, 1}},
			{"kafka-test-2", 2, []BrokerID{3, 2}},
		}

		cluster := NewMatchingDistStrategy()
		actual, err := cluster.Assignments(configs)
		if assert.NoError(t, err) {
			sort.Sort(byPartitionInPartitionReplicas(expected))
			sort.Sort(byPartitionInPartitionReplicas(actual))
			assert.EqualValues(t, expected, actual)
		}
	})
}
