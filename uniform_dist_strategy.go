package kafkactl

import (
	"time"

	"math/rand"

	"github.com/pkg/errors"
)

type UniformDistStrategy struct {
	cluster   *Cluster
	name      string
	topics    []string
	brokerIDs []BrokerID
}

func init() {
	rand.Seed(time.Now().Unix())
}

func NewUniformDistStrategy(configs StrategyConfigs) *UniformDistStrategy {
	return &UniformDistStrategy{
		cluster:   configs.Cluster,
		name:      configs.Name,
		topics:    configs.Topics,
		brokerIDs: configs.Brokers,
	}
}

func (uds *UniformDistStrategy) Name() string { return uds.name }

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

	// Set of all bidSet available for serving partitions
	bidSet := buildBrokerIdSet(brokers, isRackAware)

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
						if j > bidSet.Size() {
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
		// move a new broker for the next iteration
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
			TopicPartition: tp,
			Replicas:       bids.GetAll(),
		})
	}
	return prs
}

func buildBrokerIdSet(brokers []Broker, isRackAware bool) *BrokerIDTreeSet {
	bset := NewBrokerIDSet()
	for _, b := range brokers {
		bset.Add(b.Id)
	}
	return bset
}
