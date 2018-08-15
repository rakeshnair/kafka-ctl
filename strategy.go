package kafkactl

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
)

// ErrTopicsMissing is thrown when we attempt to create a strategy without specifying any topic names
var ErrTopicsMissing = errors.New("topics missing for strategy")

// Strategy is the interface that exposes method to implement a partition distribution
type Strategy interface {
	Assignments() ([]PartitionDistribution, error)
}

type StrategyConfigs struct {
	// TopicPartitions is the list of Partitions that will be considered for rebalancing
	TopicPartitions []TopicPartitionInfo

	// Brokers is the list of Broker objects involved in the rebalancing
	Brokers []Broker
}

type topicPartition struct {
	Topic     string `json:"topic"`
	Partition int64  `json:"partition"`
}

func (tp topicPartition) ToString() string {
	return fmt.Sprintf("%s-%d", tp.Topic, tp.Partition)
}

// PartitionReplicasDiff returns the difference between two sets of PartitionReplica slices
func PartitionReplicasDiff(old []PartitionDistribution, new []PartitionDistribution) []PartitionDistribution {
	var diff []PartitionDistribution
	mp := map[topicPartition][]BrokerID{}
	for _, pr := range old {
		mp[topicPartition{Topic: pr.Topic, Partition: pr.Partition}] = pr.Replicas
	}

	for _, pr := range new {
		tp := topicPartition{Topic: pr.Topic, Partition: pr.Partition}
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

func toPartitionReplicas(tpMap map[topicPartition]*BrokerIDTreeSet) []PartitionDistribution {
	var prs []PartitionDistribution
	for tp, bids := range tpMap {
		prs = append(prs, PartitionDistribution{
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

func groupByTopic(tps []TopicPartitionInfo) map[string][]TopicPartitionInfo {
	mp := map[string][]TopicPartitionInfo{}
	for _, tp := range tps {
		current, exists := mp[tp.Topic]
		if !exists {
			mp[tp.Topic] = []TopicPartitionInfo{tp}
			continue
		}
		current = append(current, tp)
		mp[tp.Topic] = current
	}
	return mp
}

// -- Sort helpers
type byPartitionInPartitionReplicas []PartitionDistribution

func (p byPartitionInPartitionReplicas) Len() int { return len(p) }

func (p byPartitionInPartitionReplicas) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

func (p byPartitionInPartitionReplicas) Less(i, j int) bool { return p[i].Partition < p[j].Partition }
