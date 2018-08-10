package kafkactl

type Strategy interface {
	Name() string
	Assignments() ([]PartitionReplicas, error)
}

type StrategyConfigs struct {
	Cluster *Cluster   `json:"cluster"`
	Name    string     `json:"name"`
	Topics  []string   `json:"topics"`
	Brokers []BrokerID `json:"brokerIDs"`
}
