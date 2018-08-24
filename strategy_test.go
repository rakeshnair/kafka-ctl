package kafkactl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStrategy_toPartitionReplicas(t *testing.T) {
	input := map[topicPartition]*BrokerIDTreeSet{}
	input[topicPartition{"topic1", 0}] = func() *BrokerIDTreeSet {
		set := NewBrokerIDSet()
		set.entries = []BrokerID{1, 3, 4, 5}
		return set
	}()
	input[topicPartition{"topic2", 1}] = func() *BrokerIDTreeSet {
		set := NewBrokerIDSet()
		set.entries = []BrokerID{3, 4}
		return set
	}()
	expected := []PartitionReplicas{
		{"topic1", 0, []BrokerID{1, 3, 4, 5}},
		{"topic2", 1, []BrokerID{3, 4}},
	}
	actual := generatePartitionDistribution(input)
	assert.EqualValues(t, expected, actual)
}

func TestStrategy_buildBrokerIDSet(t *testing.T) {
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

func TestStrategy_mergeN(t *testing.T) {
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

func TestStrategy_PartitionReplicasDiff(t *testing.T) {
	tests := []struct {
		old      []PartitionReplicas
		new      []PartitionReplicas
		expected []PartitionReplicas
	}{
		{
			old: []PartitionReplicas{
				{"kafka-topic-1", 0, []BrokerID{0, 1, 2}},
				{"kafka-topic-1", 1, []BrokerID{1, 2, 3}},
				{"kafka-topic-1", 2, []BrokerID{2, 1, 0}},
			},
			new: []PartitionReplicas{
				{"kafka-topic-1", 0, []BrokerID{0, 1, 2}},
				{"kafka-topic-1", 1, []BrokerID{1, 2, 3}},
				{"kafka-topic-1", 2, []BrokerID{2, 1, 0}},
			},
			expected: []PartitionReplicas{},
		},
		{
			old: []PartitionReplicas{
				{"kafka-topic-1", 0, []BrokerID{0, 1, 2}},
				{"kafka-topic-1", 1, []BrokerID{1, 2, 3}},
				{"kafka-topic-1", 2, []BrokerID{2, 1, 0}},
			},
			new: []PartitionReplicas{
				{"kafka-topic-1", 0, []BrokerID{0, 1, 2}},
				{"kafka-topic-1", 1, []BrokerID{1, 2, 3}},
				{"kafka-topic-1", 2, []BrokerID{1, 2, 0}},
			},
			expected: []PartitionReplicas{
				{"kafka-topic-1", 2, []BrokerID{1, 2, 0}},
			},
		},
		{
			old: []PartitionReplicas{
				{"kafka-topic-1", 0, []BrokerID{0, 1, 2}},
			},
			new: []PartitionReplicas{
				{"kafka-topic-1", 0, []BrokerID{0, 1, 2}},
				{"kafka-topic-1", 1, []BrokerID{1, 2, 3}},
				{"kafka-topic-1", 2, []BrokerID{1, 2, 0}},
			},
			expected: []PartitionReplicas{
				{"kafka-topic-1", 1, []BrokerID{1, 2, 3}},
				{"kafka-topic-1", 2, []BrokerID{1, 2, 0}},
			},
		},
		{
			old: []PartitionReplicas{
				{"kafka-topic-1", 0, []BrokerID{0, 1, 2}},
			},
			new: []PartitionReplicas{
				{"kafka-topic-1", 0, []BrokerID{0, 2, 1}},
				{"kafka-topic-1", 1, []BrokerID{1, 2, 3}},
			},
			expected: []PartitionReplicas{
				{"kafka-topic-1", 0, []BrokerID{0, 2, 1}},
				{"kafka-topic-1", 1, []BrokerID{1, 2, 3}},
			},
		},
	}

	for index, test := range tests {
		t.Run(indexedScenario(index), func(t *testing.T) {
			actual := PartitionReplicasDiff(test.old, test.new)
			if len(test.expected) == 0 {
				assert.True(t, (len(actual) == 0))
				return
			}

			assert.Equal(t, test.expected, actual)
		})
	}
}

func TestStrategy_groupByTopic(t *testing.T) {
	tests := []struct {
		input    []TopicPartitionInfo
		expected map[string][]TopicPartitionInfo
	}{
		{
			input: []TopicPartitionInfo{
				{Topic: "kafka-test-1", Partition: 0, Replication: 2, Leader: 3},
				{Topic: "kafka-test-1", Partition: 1, Replication: 2, Leader: 2},
				{Topic: "kafka-test-1", Partition: 2, Replication: 2, Leader: 1},
				{Topic: "kafka-test-2", Partition: 0, Replication: 2, Leader: 1},
				{Topic: "kafka-test-2", Partition: 1, Replication: 2, Leader: 2},
			},
			expected: map[string][]TopicPartitionInfo{
				"kafka-test-1": []TopicPartitionInfo{
					{Topic: "kafka-test-1", Partition: 0, Replication: 2, Leader: 3},
					{Topic: "kafka-test-1", Partition: 1, Replication: 2, Leader: 2},
					{Topic: "kafka-test-1", Partition: 2, Replication: 2, Leader: 1},
				},
				"kafka-test-2": []TopicPartitionInfo{
					{Topic: "kafka-test-2", Partition: 0, Replication: 2, Leader: 1},
					{Topic: "kafka-test-2", Partition: 1, Replication: 2, Leader: 2},
				},
			},
		},
		{
			input: []TopicPartitionInfo{
				{Topic: "kafka-test-1", Partition: 0, Replication: 2, Leader: 3},
				{Topic: "kafka-test-1", Partition: 1, Replication: 2, Leader: 2},
				{Topic: "kafka-test-1", Partition: 2, Replication: 2, Leader: 1},
			},
			expected: map[string][]TopicPartitionInfo{
				"kafka-test-1": []TopicPartitionInfo{
					{Topic: "kafka-test-1", Partition: 0, Replication: 2, Leader: 3},
					{Topic: "kafka-test-1", Partition: 1, Replication: 2, Leader: 2},
					{Topic: "kafka-test-1", Partition: 2, Replication: 2, Leader: 1},
				},
			},
		},
		{
			input:    []TopicPartitionInfo{},
			expected: map[string][]TopicPartitionInfo{},
		},
	}

	for index, test := range tests {
		t.Run(indexedScenario(index), func(t *testing.T) {
			actual := groupByTopic(test.input)
			assert.EqualValues(t, test.expected, actual)
		})
	}
}
