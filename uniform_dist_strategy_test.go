package kafkactl

import (
	"testing"

	"sort"

	"github.com/stretchr/testify/assert"
)

//go:generate mockery -dir . -outpkg kafkactl -inpkg -output . -case underscore -name=ClusterAPI

func TestUniformDistStrategy_Assignments(t *testing.T) {
	defer func() { randomStartIndexFn = randomStartIndex }()

	t.Run("singe_topic", func(t *testing.T) {
		topics := []string{"kafka-test-1"}
		expected := []PartitionReplicas{{"kafka-test-1", 0, []BrokerID{1, 3}}, {"kafka-test-1", 1, []BrokerID{3, 2}}, {"kafka-test-1", 2, []BrokerID{2, 1}}}

		mockCluster := &MockClusterAPI{}
		uds := &UniformDistStrategy{
			topics:  topics,
			cluster: mockCluster,
		}
		randomStartIndexFn = func(max int) int { return 0 }
		mockCluster.On("Brokers").Return([]Broker{{Id: 1, Rack: "1"}, {Id: 2, Rack: "1"}, {Id: 3, Rack: "2"}}, nil)
		mockCluster.On("DescribeTopic", "kafka-test-1").Return(
			[]TopicPartitionInfo{
				{TopicPartition: TopicPartition{Topic: "kafka-test-1", Partition: 0}, Replication: 2},
				{TopicPartition: TopicPartition{Topic: "kafka-test-1", Partition: 1}, Replication: 2},
				{TopicPartition: TopicPartition{Topic: "kafka-test-1", Partition: 2}, Replication: 2},
			}, nil)

		actual, err := uds.Assignments()
		if assert.NoError(t, err) {
			sort.Sort(byPartitionInPartitionReplicas(expected))
			assert.EqualValues(t, expected, actual)
		}
	})

	t.Run("two_topics", func(t *testing.T) {
		topics := []string{"kafka-test-1", "kafka-test-2"}
		expected := []PartitionReplicas{
			{"kafka-test-1", 0, []BrokerID{1, 3}},
			{"kafka-test-1", 1, []BrokerID{3, 2}},
			{"kafka-test-1", 2, []BrokerID{2, 1}},
			{"kafka-test-2", 0, []BrokerID{1, 3}},
			{"kafka-test-2", 1, []BrokerID{3, 2}}}

		mockCluster := &MockClusterAPI{}
		uds := &UniformDistStrategy{
			topics:  topics,
			cluster: mockCluster,
		}
		randomStartIndexFn = func(max int) int { return 0 }
		mockCluster.On("Brokers").Return([]Broker{{Id: 1, Rack: "1"}, {Id: 2, Rack: "1"}, {Id: 3, Rack: "2"}}, nil)
		mockCluster.On("DescribeTopic", "kafka-test-1").Return(
			[]TopicPartitionInfo{
				{TopicPartition: TopicPartition{Topic: "kafka-test-1", Partition: 0}, Replication: 2},
				{TopicPartition: TopicPartition{Topic: "kafka-test-1", Partition: 1}, Replication: 2},
				{TopicPartition: TopicPartition{Topic: "kafka-test-1", Partition: 2}, Replication: 2},
			}, nil)
		mockCluster.On("DescribeTopic", "kafka-test-2").Return(
			[]TopicPartitionInfo{
				{TopicPartition: TopicPartition{Topic: "kafka-test-2", Partition: 0}, Replication: 2},
				{TopicPartition: TopicPartition{Topic: "kafka-test-2", Partition: 1}, Replication: 2},
			}, nil)

		actual, err := uds.Assignments()
		if assert.NoError(t, err) {
			sort.Sort(byPartitionInPartitionReplicas(expected))
			assert.EqualValues(t, expected, actual)
		}
	})
}

func TestUniformDistStrategy_topicAssignments(t *testing.T) {
	defer func() { randomStartIndexFn = randomStartIndex }()

	t.Run("no_rack_2_node_cluster", func(t *testing.T) {
		topic := "kafka-test-1"
		expected := []PartitionReplicas{{"kafka-test-1", 0, []BrokerID{1, 2}}, {"kafka-test-1", 1, []BrokerID{2, 1}}}

		mockCluster := &MockClusterAPI{}
		uds := &UniformDistStrategy{
			cluster: mockCluster,
		}
		randomStartIndexFn = func(max int) int { return 0 }
		mockCluster.On("Brokers").Return([]Broker{{Id: 1, Rack: "1"}, {Id: 2, Rack: "1"}}, nil)
		mockCluster.On("DescribeTopic", "kafka-test-1").Return(
			[]TopicPartitionInfo{
				{TopicPartition: TopicPartition{Topic: "kafka-test-1", Partition: 0}, Replication: 2},
				{TopicPartition: TopicPartition{Topic: "kafka-test-1", Partition: 1}, Replication: 2},
			}, nil)

		actual, err := uds.topicAssignments(topic)
		if assert.NoError(t, err) {
			sort.Sort(byPartitionInPartitionReplicas(expected))
			assert.EqualValues(t, actual, expected)
		}
	})

	t.Run("rack_aware_3_node_cluster", func(t *testing.T) {
		topic := "kafka-test-1"
		expected := []PartitionReplicas{{"kafka-test-1", 0, []BrokerID{1, 3}}, {"kafka-test-1", 1, []BrokerID{3, 2}}, {"kafka-test-1", 2, []BrokerID{2, 1}}}

		mockCluster := &MockClusterAPI{}
		uds := &UniformDistStrategy{
			cluster: mockCluster,
		}
		randomStartIndexFn = func(max int) int { return 0 }
		mockCluster.On("Brokers").Return([]Broker{{Id: 1, Rack: "1"}, {Id: 2, Rack: "1"}, {Id: 3, Rack: "2"}}, nil)
		mockCluster.On("DescribeTopic", "kafka-test-1").Return(
			[]TopicPartitionInfo{
				{TopicPartition: TopicPartition{Topic: "kafka-test-1", Partition: 0}, Replication: 2},
				{TopicPartition: TopicPartition{Topic: "kafka-test-1", Partition: 1}, Replication: 2},
				{TopicPartition: TopicPartition{Topic: "kafka-test-1", Partition: 2}, Replication: 2},
			}, nil)

		actual, err := uds.topicAssignments(topic)
		if assert.NoError(t, err) {
			sort.Sort(byPartitionInPartitionReplicas(expected))
			assert.EqualValues(t, expected, actual)
		}
	})

	t.Run("rack_aware_3_node_cluster_index_1", func(t *testing.T) {
		topic := "kafka-test-1"
		expected := []PartitionReplicas{{"kafka-test-1", 0, []BrokerID{3, 2}}, {"kafka-test-1", 1, []BrokerID{2, 1}}, {"kafka-test-1", 2, []BrokerID{1, 3}}}

		mockCluster := &MockClusterAPI{}
		uds := &UniformDistStrategy{
			cluster: mockCluster,
		}
		randomStartIndexFn = func(max int) int { return 1 }
		mockCluster.On("Brokers").Return([]Broker{{Id: 1, Rack: "1"}, {Id: 2, Rack: "1"}, {Id: 3, Rack: "2"}}, nil)
		mockCluster.On("DescribeTopic", "kafka-test-1").Return(
			[]TopicPartitionInfo{
				{TopicPartition: TopicPartition{Topic: "kafka-test-1", Partition: 0}, Replication: 2},
				{TopicPartition: TopicPartition{Topic: "kafka-test-1", Partition: 1}, Replication: 2},
				{TopicPartition: TopicPartition{Topic: "kafka-test-1", Partition: 2}, Replication: 2},
			}, nil)

		actual, err := uds.topicAssignments(topic)
		if assert.NoError(t, err) {
			sort.Sort(byPartitionInPartitionReplicas(expected))
			assert.EqualValues(t, expected, actual)
		}
	})

	t.Run("rack_aware_3_node_cluster_replication_3", func(t *testing.T) {
		topic := "kafka-test-1"
		expected := []PartitionReplicas{{"kafka-test-1", 0, []BrokerID{1, 3, 2}}, {"kafka-test-1", 1, []BrokerID{3, 2, 1}}, {"kafka-test-1", 2, []BrokerID{2, 1, 3}}}

		mockCluster := &MockClusterAPI{}
		uds := &UniformDistStrategy{
			cluster: mockCluster,
		}
		randomStartIndexFn = func(max int) int { return 0 }
		mockCluster.On("Brokers").Return([]Broker{{Id: 1, Rack: "1"}, {Id: 2, Rack: "1"}, {Id: 3, Rack: "2"}}, nil)
		mockCluster.On("DescribeTopic", "kafka-test-1").Return(
			[]TopicPartitionInfo{
				{TopicPartition: TopicPartition{Topic: "kafka-test-1", Partition: 0}, Replication: 3},
				{TopicPartition: TopicPartition{Topic: "kafka-test-1", Partition: 1}, Replication: 3},
				{TopicPartition: TopicPartition{Topic: "kafka-test-1", Partition: 2}, Replication: 3},
			}, nil)

		actual, err := uds.topicAssignments(topic)
		if assert.NoError(t, err) {
			sort.Sort(byPartitionInPartitionReplicas(expected))
			assert.EqualValues(t, expected, actual)
		}
	})
}

func TestUniformDistStrategy_toPartitionReplicas(t *testing.T) {
	input := map[TopicPartition]*BrokerIDTreeSet{}
	input[TopicPartition{"topic1", 0}] = func() *BrokerIDTreeSet {
		set := NewBrokerIDSet()
		set.entries = []BrokerID{1, 3, 4, 5}
		return set
	}()
	input[TopicPartition{"topic2", 1}] = func() *BrokerIDTreeSet {
		set := NewBrokerIDSet()
		set.entries = []BrokerID{3, 4}
		return set
	}()
	expected := []PartitionReplicas{
		{"topic1", 0, []BrokerID{1, 3, 4, 5}},
		{"topic2", 1, []BrokerID{3, 4}},
	}
	actual := toPartitionReplicas(input)
	assert.EqualValues(t, expected, actual)
}

func TestUniformDistStrategy_buildBrokerIDSet(t *testing.T) {
	tests := []struct {
		input       []Broker
		isRackAware bool
		expected    []BrokerID
	}{
		{[]Broker{}, false, []BrokerID{}},
		{[]Broker{{Id: 1}, {Id: 2}, {Id: 3}}, false, []BrokerID{1, 2, 3}},
		{[]Broker{{Id: 1, Rack: "1"}, {Id: 2, Rack: "1"}, {Id: 3, Rack: "1"}}, true, []BrokerID{1, 2, 3}},
		{[]Broker{{Id: 1, Rack: "1"}, {Id: 2, Rack: "1"}, {Id: 3, Rack: "2"}}, true, []BrokerID{1, 3, 2}},
		{[]Broker{{Id: 1, Rack: "1"}, {Id: 2, Rack: "1"}, {Id: 3, Rack: "2"}, {Id: 4, Rack: "3"}}, true, []BrokerID{1, 3, 4, 2}},
	}
	for _, test := range tests {
		actual := buildBrokerIDSet(test.input, test.isRackAware)
		if len(test.expected) == 0 {
			assert.Equal(t, 0, len(actual.entries))
			continue
		}
		assert.EqualValues(t, test.expected, actual.entries)
	}
}

func TestUniformDistStrategy_mergeN(t *testing.T) {
	tests := []struct {
		input    [][]BrokerID
		expected []BrokerID
	}{
		{[][]BrokerID{}, []BrokerID{}},
		{[][]BrokerID{{1, 2}, {3}, {4, 5, 6}, {7, 8}}, []BrokerID{1, 3, 4, 7, 2, 5, 8, 6}},
		{[][]BrokerID{{1, 2}, {3}}, []BrokerID{1, 3, 2}},
		{[][]BrokerID{{1, 2}}, []BrokerID{1, 2}},
		{[][]BrokerID{{1}}, []BrokerID{1}},
		{[][]BrokerID{{1}, {2}, {3}}, []BrokerID{1, 2, 3}},
	}

	for _, test := range tests {
		actual := mergeN(test.input...)
		if len(test.expected) == 0 {
			assert.Equal(t, 0, len(actual))
			continue
		}
		assert.EqualValues(t, test.expected, actual)
	}
}
