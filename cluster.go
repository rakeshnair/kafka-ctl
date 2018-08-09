package kafkactl

import (
	"fmt"

	"strconv"
	"strings"

	"os"

	"github.com/olekukonko/tablewriter"
	"github.com/segmentio/objconv/json"
)

var (
	BrokerIdsPath         = "/brokers/ids"
	ControllerPath        = "/controller"
	ClusterIdPath         = "/cluster/id"
	TopicPath             = "/brokers/topics"
	TopicPathTemplate     = "/brokers/topics/{topic}/partitions"
	PartitionPathTemplate = "/brokers/topics/{topic}/partitions/{partition}/state"
)

// BrokerID uniquely identifies a broker in a Kafka cluster
type BrokerID int64

// Cluster provides a client to interact with a Kafka cluster
type Cluster struct {
	store ClusterMetaStore
}

// Broker wraps all metadata information for a single Kafka broker
type Broker struct {
	Id                          BrokerID          `json:"id"`
	ListenerSecurityProtocolMap map[string]string `json:"listener_security_protocol_map"`
	Endpoints                   []string          `json:"endpoints"`
	Rack                        string            `json:"rack"`
	JmxPort                     int32             `json:"jmx_port"`
	Host                        string            `json:"host"`
	Port                        int32             `json:"port"`
	Timestamp                   int64             `json:"timestamp"`
	Version                     int               `json:"version"`
}

// TopicPartition wraps all metadata information for a single Kafka partition
type TopicPartition struct {
	Topic     string     `json:"topic"`
	Partition int64      `json:"partition"`
	Leader    BrokerID   `json:"leader"`
	Replicas  []BrokerID `json:"replicas"`
	ISR       []BrokerID `json:"isr"`
}

// NewCluster returns a new client to interact with a Kafka cluster
func NewCluster(store ClusterMetaStore) *Cluster {
	return &Cluster{store: store}
}

type clusterInfo struct {
	Version int    `json:"version"`
	Id      string `json:"id"`
}

// ID returns the cluster identifier
func (c *Cluster) ID() (string, error) {
	data, err := c.store.Get(ClusterIdPath)
	if err != nil {
		return "", err
	}
	var cinfo clusterInfo
	err = json.Unmarshal(data, &cinfo)
	if err != nil {
		return "", err
	}
	return cinfo.Id, nil
}

type controller struct {
	BrokerId  BrokerID `json:"brokerid"`
	Version   int      `json:"version"`
	Timestamp int64    `json:"timestamp"`
}

// Controller returns the broker which is currently acting as the controller
func (c *Cluster) Controller() (Broker, error) {
	data, err := c.store.Get(ControllerPath)
	if err != nil {
		return Broker{}, err
	}
	var ctlr controller
	err = json.Unmarshal(data, &ctlr)
	if err != nil {
		return Broker{}, err
	}
	return c.Broker(ctlr.BrokerId)
}

// Brokers returns the list of all brokers in the cluster
func (c *Cluster) Brokers() ([]Broker, error) {
	ids, err := c.store.List(BrokerIdsPath)
	if err != nil {
		return []Broker{}, err
	}
	var brokers []Broker
	for _, id := range ids {
		vals := strings.Split(id, "/")
		i, err := strconv.ParseInt(vals[len(vals)-1], 10, 64)
		if err != nil {
			return []Broker{}, err
		}
		b, err := c.Broker(BrokerID(i))
		if err != nil {
			return []Broker{}, err
		}
		brokers = append(brokers, b)
	}
	return brokers, nil
}

// Broker returns the information about the broker with the input id
func (c *Cluster) Broker(id BrokerID) (Broker, error) {
	path := fmt.Sprintf("%s/%d", BrokerIdsPath, id)
	data, err := c.store.Get(path)
	if err != nil {
		return Broker{}, err
	}
	var b Broker
	err = json.Unmarshal(data, &b)
	if err != nil {
		return Broker{}, err
	}
	b.Id = id
	return b, nil
}

// Topics returns the list of all topic names registered with the cluster
func (c *Cluster) Topics() ([]string, error) {
	tps, err := c.store.List(TopicPath)
	if err != nil {
		return []string{}, err
	}
	var topics []string
	for _, tp := range tps {
		topics = append(topics, tp[strings.LastIndex(tp, "/")+1:])
	}
	return topics, nil
}

type replicas struct {
	Version    int                   `json:"version"`
	Partitions map[string][]BrokerID `json:"partitions"`
}

func (c *Cluster) topicReplicas(name string) (map[int64][]BrokerID, error) {
	path := fmt.Sprintf("%s/%s", TopicPath, name)
	data, err := c.store.Get(path)
	if err != nil {
		return map[int64][]BrokerID{}, err
	}
	var r replicas
	err = json.Unmarshal(data, &r)
	if err != nil {
		return map[int64][]BrokerID{}, err
	}
	res := map[int64][]BrokerID{}
	for k, v := range r.Partitions {
		p, err := strconv.ParseInt(k, 10, 64)
		if err != nil {
			return map[int64][]BrokerID{}, err
		}
		res[p] = v
	}
	return res, nil
}

// TopicInfo returns the list of all partitions associated with the topic
func (c *Cluster) TopicInfo(name string) ([]TopicPartition, error) {
	replicas, err := c.topicReplicas(name)
	if err != nil {
		return []TopicPartition{}, err
	}

	s := strings.Replace(TopicPathTemplate, "{topic}", name, 1)
	pids, err := c.store.List(s)
	if err != nil {
		return []TopicPartition{}, err
	}
	var tps []TopicPartition

	withTopic := strings.Replace(PartitionPathTemplate, "{topic}", name, 1)
	for _, pid := range pids {
		pidpath := strings.Replace(withTopic, "{partition}", pid, 1)
		data, err := c.store.Get(pidpath)
		if err != nil {
			return []TopicPartition{}, err
		}
		var tp TopicPartition
		err = json.Unmarshal(data, &tp)
		if err != nil {
			return []TopicPartition{}, err
		}
		tp.Topic = name
		tp.Partition, err = strconv.ParseInt(pid, 10, 64)
		if err != nil {
			return []TopicPartition{}, err
		}
		tp.Replicas = replicas[tp.Partition]
		tps = append(tps, tp)
	}
	return tps, nil
}

func filterByBrokerID(id BrokerID, tps []TopicPartition) []TopicPartition {
	var res []TopicPartition
	for _, tp := range tps {
		if tp.Leader == id {
			res = append(res, tp)
		}
	}
	return res
}

// TopicsWithDetails returns the list of all partitions associated with all the topics in the cluster
func (c *Cluster) TopicsWithDetails() ([]TopicPartition, error) {
	topics, err := c.Topics()
	if err != nil {
		return []TopicPartition{}, err
	}

	var res []TopicPartition
	for _, t := range topics {
		tpinfo, err := c.TopicInfo(t)
		if err != nil {
			return []TopicPartition{}, err
		}
		res = append(res, tpinfo...)
	}
	return res, nil
}

// TopicsForBroker returns the list of all partitions associated with all the topics in the broker
func (c *Cluster) TopicsForBroker(id BrokerID) ([]TopicPartition, error) {
	tps, err := c.TopicsWithDetails()
	if err != nil {
		return []TopicPartition{}, err
	}
	return filterByBrokerID(id, tps), nil
}

// TopicBrokerDistribution contains distribution of partitions for a specific topic
// in a given broker
type TopicBrokerDistribution struct {
	Topic    string   `json:"topic"`
	ID       BrokerID `json:"id"`
	Leaders  []int64  `json:"leaders"`
	Replicas []int64  `json:"replicas"`
}

// PartitionDistribution for a specific topic indicates how the leaders and replicas are
// distributed among the available brokers in the cluster
func (c *Cluster) PartitionDistribution(topic string) ([]TopicBrokerDistribution, error) {
	brokers, err := c.Brokers()
	if err != nil {
		return []TopicBrokerDistribution{}, err
	}

	leaderMap := map[BrokerID]*Int64List{}
	replicaMap := map[BrokerID]*Int64List{}
	for _, broker := range brokers {
		leaderMap[broker.Id] = newInt64List()
		replicaMap[broker.Id] = newInt64List()
	}

	tps, err := c.TopicInfo(topic)
	if err != nil {
		return []TopicBrokerDistribution{}, err
	}

	for _, tp := range tps {
		leaderMap[tp.Leader].Add(tp.Partition)
		for _, replica := range tp.Replicas {
			if replica != tp.Leader {
				replicaMap[replica].Add(tp.Partition)
			}
		}
	}

	var pds []TopicBrokerDistribution
	for _, broker := range brokers {
		pds = append(pds, TopicBrokerDistribution{
			Topic:    topic,
			ID:       broker.Id,
			Leaders:  leaderMap[broker.Id].GetAll(),
			Replicas: replicaMap[broker.Id].GetAll(),
		})
	}
	return pds, nil
}

func PrettyPrintPartitionDistribution(pds []TopicBrokerDistribution) {
	tw := tablewriter.NewWriter(os.Stdout)
	tw.SetHeader([]string{"BrokerID", "Leaders", "Replicas"})

	for _, pd := range pds {
		var row []string
		row = append(row, fmt.Sprintf("%d", pd.ID))
		row = append(row, strings.Trim(strings.Join(strings.Fields(fmt.Sprint(pd.Leaders)), ","), "[]"))
		row = append(row, strings.Trim(strings.Join(strings.Fields(fmt.Sprint(pd.Replicas)), ","), "[]"))
		tw.Append(row)
	}

	tw.Render()
}

type Int64List struct {
	entries []int64
}

func newInt64List() *Int64List {
	l := &Int64List{}
	l.entries = []int64{}
	return l
}

func (l *Int64List) Add(entry int64) {
	l.entries = append(l.entries, entry)
}

func (l *Int64List) GetAll() []int64 {
	return l.entries
}
