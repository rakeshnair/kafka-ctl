package kafkactl

import "errors"

// BrokerID uniquely identifies a broker in a Kafka cluster
type BrokerID int64

// BrokerIDSet is a set implementation to store BrokerID values
type BrokerIDSet struct {
	idmap   map[BrokerID]bool
	entries []BrokerID
	index   int
}

func NewBrokerIDSet() *BrokerIDSet {
	bset := &BrokerIDSet{}
	bset.idmap = map[BrokerID]bool{}
	return bset
}

func (bset *BrokerIDSet) Add(entry BrokerID) bool {
	if !bset.idmap[entry] {
		bset.idmap[entry] = true
		bset.entries = append(bset.entries, entry)
		return true
	}
	return false
}

func (bset *BrokerIDSet) Entries() []BrokerID {
	return bset.entries
}

var ErrIndexOutOfBounds = errors.New("index out of bound")

func (bset *BrokerIDSet) Get(index int) (BrokerID, error) {
	if len(bset.entries) == 0 {
		return -1, errors.New("brokerIDs set is empty")
	}
	if index >= len(bset.entries) {
		return -1, ErrIndexOutOfBounds
	}
	return bset.entries[index], nil
}

func (bset *BrokerIDSet) Length() int {
	return len(bset.entries)
}
