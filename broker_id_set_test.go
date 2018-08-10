package kafkactl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBrokerIDTreeSet_Add(t *testing.T) {
	tests := []struct {
		seed          []BrokerID
		input         BrokerID
		expectedRes   bool
		expectedState []BrokerID
	}{
		{[]BrokerID{}, 1, true, []BrokerID{1}},
		{[]BrokerID{1, 3}, 1, false, []BrokerID{1, 3}},
		{[]BrokerID{1, 2, 3}, 4, true, []BrokerID{1, 2, 3, 4}},
	}

	for _, test := range tests {
		set := NewBrokerIDSet()
		fillSet(set, test.seed)
		actualRes := set.Add(test.input)
		assert.Equal(t, test.expectedRes, actualRes)
		assert.EqualValues(t, test.expectedState, set.entries)
	}
}

func TestBrokerIDTreeSet_GetAll(t *testing.T) {
	tests := []struct {
		seed          []BrokerID
		expectedState []BrokerID
	}{
		{[]BrokerID{}, []BrokerID{}},
		{[]BrokerID{1, 2, 3}, []BrokerID{1, 2, 3}},
	}
	for _, test := range tests {
		set := NewBrokerIDSet()
		fillSet(set, test.seed)
		actual := set.GetAll()
		if len(test.expectedState) == 0 {
			assert.Equal(t, 0, len(actual))
			continue
		}
		assert.Equal(t, test.expectedState, actual)
	}
}

func TestBrokerIDTreeSet_Get(t *testing.T) {
	tests := []struct {
		seed     []BrokerID
		input    int
		expected BrokerID
		error    error
	}{
		{[]BrokerID{1, 2, 3}, 0, 1, nil},
		{[]BrokerID{1, 2, 3}, 2, 3, nil},
		{[]BrokerID{}, 0, -1, ErrSetEmpty},
		{[]BrokerID{1, 2, 3}, 3, -1, ErrSetIndexOutOfBounds},
	}
	for _, test := range tests {
		set := NewBrokerIDSet()
		fillSet(set, test.seed)
		actualRes, actualErr := set.Get(test.input)
		assert.Equal(t, test.error, actualErr)
		assert.Equal(t, test.expected, actualRes)
	}
}

func TestBrokerIDTreeSet_Size(t *testing.T) {
	tests := []struct {
		seed     []BrokerID
		expected int
	}{
		{[]BrokerID{}, 0},
		{[]BrokerID{1, 2, 3}, 3},
	}
	for _, test := range tests {
		set := NewBrokerIDSet()
		fillSet(set, test.seed)
		actual := set.Size()
		assert.Equal(t, test.expected, actual)
	}
}

func fillSet(set *BrokerIDTreeSet, items []BrokerID) {
	for _, item := range items {
		set.entryMap[item] = true
		set.entries = append(set.entries, item)
	}
}
