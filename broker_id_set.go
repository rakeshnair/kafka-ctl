package kafkactl

import "errors"

// BrokerID uniquely identifies a broker in a Kafka cluster
type BrokerID int64

// ErrSetIndexOutOfBounds is thrown while trying to access an element at
// an index greater than the max allowed index
var ErrSetIndexOutOfBounds = errors.New("index out of bound")

// BrokerIDTreeSet is a set of BrokerIDs that maintains the natural insertion order
type BrokerIDTreeSet struct {
	entryMap map[BrokerID]bool
	entries  []BrokerID
}

// NewBrokerIDSet returns a new BrokerIDTreeSet
func NewBrokerIDSet() *BrokerIDTreeSet {
	bset := &BrokerIDTreeSet{}
	bset.entryMap = map[BrokerID]bool{}
	return bset
}

// Add inserts a new BrokerID into the set. If the BrokerID already
// exists it takes no action and returns false. It returns true
// if the insert succeeds.
func (set *BrokerIDTreeSet) Add(entry BrokerID) bool {
	if !set.entryMap[entry] {
		set.entryMap[entry] = true
		set.entries = append(set.entries, entry)
		return true
	}
	return false
}

// GetAll returns all the elements in the set
func (set *BrokerIDTreeSet) GetAll() []BrokerID {
	return set.entries
}

// Get returns an element from the set at the given index
func (set *BrokerIDTreeSet) Get(index int) (BrokerID, error) {
	if len(set.entries) == 0 {
		return -1, errors.New("brokerIDs set is empty")
	}
	if index >= len(set.entries) {
		return -1, ErrSetIndexOutOfBounds
	}
	return set.entries[index], nil
}

// Size returns the total number of elements in the set
func (set *BrokerIDTreeSet) Size() int {
	return len(set.entries)
}
