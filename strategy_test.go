package kafkactl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStrategy_toPartitionReplicas(t *testing.T) {
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
