package kafkactl

type ClusterAPI interface {
	ID() (string, error)
	Controller() (Broker, error)
	Brokers() ([]Broker, error)
	Broker(id BrokerID) (Broker, error)
	Topics() ([]string, error)
	DescribeTopic(name string) ([]TopicPartitionInfo, error)
	DescribeAllTopics() ([]TopicPartitionInfo, error)
	DescribeTopicsForBroker(id BrokerID) ([]TopicPartitionInfo, error)
	PartitionDistribution(topic string) ([]TopicBrokerDistribution, error)
	PartitionReassignRequest(partitions []PartitionReplicas) ReassignmentReq
	ReassignPartitions(req ReassignmentReq) error
}
