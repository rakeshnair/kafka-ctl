package kafkactl

import (
	"testing"

	"sort"

	"github.com/stretchr/testify/assert"
)

//go:generate mockery -dir . -outpkg kafkactl -inpkg -output . -case underscore -name=ClusterAPI

func TestUniformDistStrategy_Assignments(t *testing.T) {

	t.Run("single_topic", func(t *testing.T) {
		// --input
		configs := StrategyConfigs{
			Brokers: []Broker{{Id: 1, Rack: "1"}, {Id: 2, Rack: "1"}, {Id: 3, Rack: "2"}},
			TopicPartitions: []TopicPartitionInfo{
				{Topic: "kafka-test-1", Partition: 0, Replication: 2, Leader: 3},
				{Topic: "kafka-test-1", Partition: 1, Replication: 2, Leader: 2},
				{Topic: "kafka-test-1", Partition: 2, Replication: 2, Leader: 1},
			},
		}

		// --expected
		expected := []PartitionDistribution{
			{"kafka-test-1", 0, []BrokerID{3, 2}},
			{"kafka-test-1", 1, []BrokerID{2, 1}},
			{"kafka-test-1", 2, []BrokerID{1, 3}},
		}

		cluster := NewUniformDistStrategy()
		actual, err := cluster.Assignments(configs)
		if assert.NoError(t, err) {
			sort.Sort(byPartitionInPartitionReplicas(expected))
			sort.Sort(byPartitionInPartitionReplicas(actual))
			assert.EqualValues(t, expected, actual)
		}
	})

	t.Run("two_topics", func(t *testing.T) {
		// --input
		configs := StrategyConfigs{
			Brokers: []Broker{{Id: 1, Rack: "1"}, {Id: 2, Rack: "1"}, {Id: 3, Rack: "2"}},
			TopicPartitions: []TopicPartitionInfo{
				{Topic: "kafka-test-1", Partition: 0, Replication: 2, Leader: 3},
				{Topic: "kafka-test-1", Partition: 1, Replication: 2, Leader: 2},
				{Topic: "kafka-test-1", Partition: 2, Replication: 2, Leader: 1},
				{Topic: "kafka-test-2", Partition: 0, Replication: 2, Leader: 1},
				{Topic: "kafka-test-2", Partition: 1, Replication: 2, Leader: 2},
			},
		}

		// --expected
		expected := []PartitionDistribution{
			{"kafka-test-1", 0, []BrokerID{3, 2}},
			{"kafka-test-1", 1, []BrokerID{2, 1}},
			{"kafka-test-1", 2, []BrokerID{1, 3}},
			{"kafka-test-2", 0, []BrokerID{1, 3}},
			{"kafka-test-2", 1, []BrokerID{3, 2}},
		}

		cluster := NewUniformDistStrategy()
		actual, err := cluster.Assignments(configs)
		if assert.NoError(t, err) {
			sort.Sort(byPartitionInPartitionReplicas(expected))
			sort.Sort(byPartitionInPartitionReplicas(actual))
			assert.EqualValues(t, expected, actual)
		}
	})
}

func TestUniformDistStrategy_topicAssignments(t *testing.T) {

	t.Run("no_rack_2_node_cluster", func(t *testing.T) {
		// --input
		topic := "kafka-test-1"
		brokers := []Broker{{Id: 1, Rack: "1"}, {Id: 2, Rack: "1"}}
		partitions := []TopicPartitionInfo{
			{Topic: "kafka-test-1", Partition: 0, Replication: 2, Leader: 1},
			{Topic: "kafka-test-1", Partition: 1, Replication: 2, Leader: 2},
		}

		// --expected
		expected := []PartitionDistribution{
			{"kafka-test-1", 0, []BrokerID{1, 2}},
			{"kafka-test-1", 1, []BrokerID{2, 1}},
		}

		cluster := NewUniformDistStrategy()
		actual, err := cluster.generateAssignments(topic, brokers, partitions)
		if assert.NoError(t, err) {
			sort.Sort(byPartitionInPartitionReplicas(expected))
			assert.EqualValues(t, actual, expected)
		}
	})

	t.Run("rack_aware_3_node_cluster", func(t *testing.T) {
		// --input
		topic := "kafka-test-1"
		brokers := []Broker{{Id: 1, Rack: "1"}, {Id: 2, Rack: "1"}, {Id: 3, Rack: "2"}}
		partitions := []TopicPartitionInfo{
			{Topic: "kafka-test-1", Partition: 0, Replication: 2, Leader: 1},
			{Topic: "kafka-test-1", Partition: 1, Replication: 2, Leader: 2},
			{Topic: "kafka-test-1", Partition: 2, Replication: 2, Leader: 3},
		}

		// --expected
		expected := []PartitionDistribution{
			{"kafka-test-1", 0, []BrokerID{1, 3}},
			{"kafka-test-1", 1, []BrokerID{3, 2}},
			{"kafka-test-1", 2, []BrokerID{2, 1}},
		}

		cluster := NewUniformDistStrategy()

		actual, err := cluster.generateAssignments(topic, brokers, partitions)
		if assert.NoError(t, err) {
			sort.Sort(byPartitionInPartitionReplicas(expected))
			assert.EqualValues(t, expected, actual)
		}
	})

	t.Run("rack_aware_3_node_cluster_index_1", func(t *testing.T) {
		// --input
		topic := "kafka-test-1"
		brokers := []Broker{{Id: 1, Rack: "1"}, {Id: 2, Rack: "1"}, {Id: 3, Rack: "2"}}
		partitions := []TopicPartitionInfo{
			{Topic: "kafka-test-1", Partition: 0, Replication: 2, Leader: 3},
			{Topic: "kafka-test-1", Partition: 1, Replication: 2, Leader: 2},
			{Topic: "kafka-test-1", Partition: 2, Replication: 2, Leader: 1},
		}

		// --expected
		expected := []PartitionDistribution{
			{"kafka-test-1", 0, []BrokerID{3, 2}},
			{"kafka-test-1", 1, []BrokerID{2, 1}},
			{"kafka-test-1", 2, []BrokerID{1, 3}},
		}

		cluster := NewUniformDistStrategy()

		actual, err := cluster.generateAssignments(topic, brokers, partitions)
		if assert.NoError(t, err) {
			sort.Sort(byPartitionInPartitionReplicas(expected))
			assert.EqualValues(t, expected, actual)
		}
	})

	t.Run("rack_aware_3_node_cluster_replication_3", func(t *testing.T) {
		// --input
		topic := "kafka-test-1"
		brokers := []Broker{{Id: 1, Rack: "1"}, {Id: 2, Rack: "1"}, {Id: 3, Rack: "2"}}
		partitions := []TopicPartitionInfo{
			{Topic: "kafka-test-1", Partition: 0, Replication: 3, Leader: 1},
			{Topic: "kafka-test-1", Partition: 1, Replication: 3, Leader: 1},
			{Topic: "kafka-test-1", Partition: 2, Replication: 3, Leader: 1},
		}

		// --expected
		expected := []PartitionDistribution{
			{"kafka-test-1", 0, []BrokerID{1, 3, 2}},
			{"kafka-test-1", 1, []BrokerID{3, 2, 1}},
			{"kafka-test-1", 2, []BrokerID{2, 1, 3}},
		}

		cluster := NewUniformDistStrategy()

		actual, err := cluster.generateAssignments(topic, brokers, partitions)
		if assert.NoError(t, err) {
			sort.Sort(byPartitionInPartitionReplicas(expected))
			assert.EqualValues(t, expected, actual)
		}
	})
}
