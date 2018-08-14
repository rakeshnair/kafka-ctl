package kafkactl

import (
	"errors"
	"math/rand"
	"reflect"
	"sort"
	"time"
)

func init() {
	rand.Seed(time.Now().Unix())
}

// ErrTopicsMissing is thrown when we attempt to create a strategy without specifying any topic names
var ErrTopicsMissing = errors.New("topics missing for strategy")

// Strategy is the interface that exposes method to implement a partition distribution
type Strategy interface {
	Assignments() ([]PartitionReplicas, error)
}

// PartitionReplicasDiff returns the difference between two sets of PartitionReplica slices
func PartitionReplicasDiff(old []PartitionReplicas, new []PartitionReplicas) []PartitionReplicas {
	var diff []PartitionReplicas
	mp := map[TopicPartition][]BrokerID{}
	for _, pr := range old {
		mp[TopicPartition{Topic: pr.Topic, Partition: pr.Partition}] = pr.Replicas
	}

	for _, pr := range new {
		tp := TopicPartition{Topic: pr.Topic, Partition: pr.Partition}
		replicas, exists := mp[tp]
		if !exists {
			diff = append(diff, pr)
			continue
		}

		if !reflect.DeepEqual(replicas, pr.Replicas) {

			diff = append(diff, pr)
		}
	}

	sort.Sort(byPartitionInPartitionReplicas(diff))
	return diff
}

func toPartitionReplicas(tpMap map[TopicPartition]*BrokerIDTreeSet) []PartitionReplicas {
	var prs []PartitionReplicas
	for tp, bids := range tpMap {
		prs = append(prs, PartitionReplicas{
			Topic:     tp.Topic,
			Partition: tp.Partition,
			Replicas:  bids.GetAll(),
		})
	}
	sort.Sort(byPartitionInPartitionReplicas(prs))
	return prs
}

func buildBrokerIDSet(brokers []Broker, isRackAware bool) *BrokerIDTreeSet {
	bset := NewBrokerIDSet()

	if isRackAware {
		rackmap := map[string]*[]BrokerID{}
		for _, b := range brokers {
			if _, ok := rackmap[b.Rack]; !ok {
				rackmap[b.Rack] = &[]BrokerID{}
			}
			ids := rackmap[b.Rack]
			*ids = append(*ids, b.Id)
		}

		// gather rack names so that they can be sorted
		var racks []string
		for rack := range rackmap {
			racks = append(racks, rack)
		}
		sort.Strings(racks)

		var idsGrps [][]BrokerID
		for _, rack := range racks {
			idsGrps = append(idsGrps, *rackmap[rack])
		}

		for _, id := range mergeN(idsGrps...) {
			bset.Add(id)
		}

		return bset
	}

	// handle case where rack awareness not enabled
	for _, b := range brokers {
		bset.Add(b.Id)
	}
	return bset
}

// mergeN takes N BrokerID slice and flattens them into a single
// slice by ensuring that elements in the same index are grouped
// together
//
// for eg: mergeN({1, 2}, {3}, {4, 5, 6}, {7, 8})
// will yield {1,3,4,7,2,5,8,6}
func mergeN(groups ...[]BrokerID) []BrokerID {
	var res []BrokerID
	for {
		out, candidate := merge(groups)
		res = append(res, out...)
		if len(candidate) == 0 {
			break
		}
		groups = candidate
	}
	return res
}

func merge(groups [][]BrokerID) ([]BrokerID, [][]BrokerID) {
	var out []BrokerID
	var candidates [][]BrokerID
	for _, grp := range groups {
		if len(grp) == 0 {
			continue
		}
		item, ngrp := split(grp)
		out = append(out, item)
		candidates = append(candidates, ngrp)
	}
	return out, candidates
}

// Note: split does not check for empty slices as input
// Ensure its checked before invoking split
func split(grp []BrokerID) (BrokerID, []BrokerID) { return grp[0], grp[1:] }

var randomStartIndexFn = randomStartIndex

func randomStartIndex(max int) int { return rand.Intn(max) }

// -- Sort helpers
type byPartitionInPartitionReplicas []PartitionReplicas

func (p byPartitionInPartitionReplicas) Len() int { return len(p) }

func (p byPartitionInPartitionReplicas) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

func (p byPartitionInPartitionReplicas) Less(i, j int) bool { return p[i].Partition < p[j].Partition }
