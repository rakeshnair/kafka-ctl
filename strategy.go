package kafkactl

// Strategy is the interface that exposes method to implement a partition distribution
type Strategy interface {
	Assignments() ([]PartitionDistribution, error)
}
