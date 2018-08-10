package kafkactl

import (
	"time"

	"math/rand"

	"sort"

	"github.com/pkg/errors"
)

func init() {
	rand.Seed(time.Now().Unix())
}

// UniformDistStrategyConfigs wraps the list of configs required to initialize a UniformDistStrategy object
type UniformDistStrategyConfigs struct {
	Cluster *Cluster   `json:"cluster"`
	Topics  []string   `json:"topics"`
	Brokers []BrokerID `json:"brokerIDs"`
}

// UniformDistStrategy implements a Strategy that uniformly distributes all topic partitions among available brokers
type UniformDistStrategy struct {
	cluster   *Cluster
	name      string
	topics    []string
	brokerIDs []BrokerID
}

// NewUniformDistStrategy returns a new instance of UniformDistStrategy
func NewUniformDistStrategy(configs UniformDistStrategyConfigs) *UniformDistStrategy {
	return &UniformDistStrategy{
		cluster:   configs.Cluster,
		topics:    configs.Topics,
		brokerIDs: configs.Brokers,
	}
}

// Assignments returns a distribution where partitions are uniformly distributed among brokers
func (uds *UniformDistStrategy) Assignments() ([]PartitionReplicas, error) {
	var prs []PartitionReplicas
	for _, topic := range uds.topics {
		tprs, err := uds.topicAssignments(topic)
		if err != nil {
			return []PartitionReplicas{}, err
		}
		prs = append(prs, tprs...)
	}
	return prs, nil
}

func (uds *UniformDistStrategy) topicAssignments(topic string) ([]PartitionReplicas, error) {
	if len(uds.brokerIDs) == 0 {
		brokers, err := uds.cluster.Brokers()
		if err != nil {
			return []PartitionReplicas{}, err
		}
		uds.brokerIDs = CollectBrokerIDs(brokers)
	}

	var brokers []Broker
	isRackAware := true
	for _, bid := range uds.brokerIDs {
		broker, err := uds.cluster.Broker(bid)
		if err != nil {
			return []PartitionReplicas{}, err
		}
		brokers = append(brokers, broker)
		isRackAware = isRackAware && (len(broker.Rack) > 0)
	}

	tps, err := uds.cluster.DescribeTopic(topic)
	if err != nil {
		return []PartitionReplicas{}, err
	}

	if len(tps) == 0 {
		return []PartitionReplicas{}, errors.New("no partitions to assign")
	}

	replicationFactor := tps[0].Replication

	// Set of all BrokerIDs available for serving partitions
	bidSet := buildBrokerIDSet(brokers, isRackAware)

	// pick a random broker to start assigning partitions
	startIndex := rand.Intn(bidSet.Size())

	// Mapping between a specific partition and the bidSet holding a replica
	tpBrokerMap := map[TopicPartition]*BrokerIDTreeSet{}
	for _, tp := range tps {
		tpBrokerMap[tp.TopicPartition] = NewBrokerIDSet()
	}

	// partitions should be assigned to brokerIDs "replicationFactor" times
	for i := 0; i < replicationFactor; i++ {
		j := startIndex
		for _, tp := range tps {
			for {
				brokerID, err := bidSet.Get(j)
				if err != nil {
					switch err {
					case ErrSetIndexOutOfBounds:
						j = j + 1
						if j >= bidSet.Size() {
							j = 0
						}
						continue
					default:
						return []PartitionReplicas{}, err
					}

				}
				success := tpBrokerMap[tp.TopicPartition].Add(brokerID)
				if success {
					break
				}
				// pick next broker if its already associated with the partition
				j = j + 1
			}

			// move to next broker once the given partition has been associated with a broker
			j = j + 1
		}
		// move to a new broker for the next iteration
		startIndex = startIndex + 1
		if startIndex >= bidSet.Size() {
			startIndex = 0
		}
	}

	return toPartitionReplicas(tpBrokerMap), nil
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
func split(grp []BrokerID) (BrokerID, []BrokerID) {
	return grp[0], grp[1:]
}
