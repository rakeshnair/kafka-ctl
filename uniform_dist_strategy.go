package kafkactl

import (
	"time"

	"math/rand"

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
func (uds *UniformDistStrategy) Assignments() ([]PartitionDistribution, error) {
	var prs []PartitionDistribution
	for _, topic := range uds.topics {
		tprs, err := uds.topicAssignments(topic)
		if err != nil {
			return []PartitionDistribution{}, err
		}
		prs = append(prs, tprs...)
	}
	return prs, nil
}

func (uds *UniformDistStrategy) topicAssignments(topic string) ([]PartitionDistribution, error) {
	if len(uds.brokerIDs) == 0 {
		brokers, err := uds.cluster.Brokers()
		if err != nil {
			return []PartitionDistribution{}, err
		}
		uds.brokerIDs = CollectBrokerIDs(brokers)
	}

	var brokers []Broker
	isRackAware := true
	for _, bid := range uds.brokerIDs {
		broker, err := uds.cluster.Broker(bid)
		if err != nil {
			return []PartitionDistribution{}, err
		}
		brokers = append(brokers, broker)
		isRackAware = isRackAware && (len(broker.Rack) > 0)
	}

	tps, err := uds.cluster.DescribeTopic(topic)
	if err != nil {
		return []PartitionDistribution{}, err
	}

	if len(tps) == 0 {
		return []PartitionDistribution{}, errors.New("no partitions to assign")
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
						if j >= bidSet.Size() {
							j = 0
						}
						continue
					default:
						return []PartitionDistribution{}, err
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

func toPartitionReplicas(tpMap map[TopicPartition]*BrokerIDTreeSet) []PartitionDistribution {
	var prs []PartitionDistribution
	for tp, bids := range tpMap {
		prs = append(prs, PartitionDistribution{
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
