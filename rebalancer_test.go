package kafkactl

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

//go:generate mockery -dir . -outpkg kafkactl -inpkg -output . -case underscore -name=ClusterAPI
//go:generate mockery -dir . -outpkg kafkactl -inpkg -output . -case underscore -name=Strategy

func TestRebalancer_Generate(t *testing.T) {

	t.Run("invalid_strategy", func(t *testing.T) {
		input := RebalanceInput{
			Topics:   []string{"kafka-test-1"},
			Strategy: "invalid strategy",
		}

		rebalancer := &Rebalancer{}

		_, err := rebalancer.Generate(input)
		assert.Equal(t, ErrUnknownStrategy, err)
	})

	t.Run("pick_entire_new_assignment", func(t *testing.T) {
		strategy := "uniform-dist-strategy"
		input := RebalanceInput{
			Topics:   []string{"kafka-test-1"},
			Strategy: strategy,
		}

		expected := []PartitionReplicas{
			{"kafka-topic-1", 0, []BrokerID{1, 2}},
			{"kafka-topic-1", 1, []BrokerID{2, 1}},
		}

		mockCluster := &MockClusterAPI{}
		mockStrategy := &MockStrategy{}

		mockCluster.On("DescribeTopic", "kafka-test-1").Return(
			[]TopicPartitionInfo{
				{Topic: "kafka-test-1", Partition: 0, Replication: 2, Leader: 2},
				{Topic: "kafka-test-1", Partition: 1, Replication: 2, Leader: 1},
			}, nil)

		mockCluster.On("ReplicaDistribution", "kafka-test-1").Return(
			[]PartitionReplicas{
				{"kafka-topic-1", 0, []BrokerID{2, 1}},
				{"kafka-topic-1", 1, []BrokerID{1, 2}},
			}, nil)

		mockCluster.On("Brokers").Return(
			[]Broker{{Id: 1}, {Id: 2}}, nil)

		mockStrategy.On("Assignments", mock.Anything).Return(
			[]PartitionReplicas{
				{"kafka-topic-1", 0, []BrokerID{1, 2}},
				{"kafka-topic-1", 1, []BrokerID{2, 1}},
			}, nil)

		mockStrategy.On("Name").Return(strategy)

		rebalancer := NewRebalancer(RebalancerConfigs{
			Cluster:    mockCluster,
			Strategies: []Strategy{mockStrategy},
		})

		actual, err := rebalancer.Generate(input)
		exitOnErr(t, err)
		assert.EqualValues(t, expected, actual)
	})

	t.Run("pick_parts_of_new_assignment", func(t *testing.T) {
		strategy := "uniform-dist-strategy"
		input := RebalanceInput{
			Topics:   []string{"kafka-test-1"},
			Strategy: strategy,
		}

		expected := []PartitionReplicas{
			{"kafka-topic-1", 2, []BrokerID{3, 1}},
		}

		mockCluster := &MockClusterAPI{}
		mockStrategy := &MockStrategy{}

		mockCluster.On("DescribeTopic", "kafka-test-1").Return(
			[]TopicPartitionInfo{
				{Topic: "kafka-test-1", Partition: 0, Replication: 2, Leader: 2},
				{Topic: "kafka-test-1", Partition: 1, Replication: 2, Leader: 1},
			}, nil)

		mockCluster.On("ReplicaDistribution", "kafka-test-1").Return(
			[]PartitionReplicas{
				{"kafka-topic-1", 0, []BrokerID{2, 1}},
				{"kafka-topic-1", 1, []BrokerID{1, 2}},
			}, nil)

		mockCluster.On("Brokers").Return(
			[]Broker{{Id: 1}, {Id: 2}}, nil)

		mockStrategy.On("Assignments", mock.Anything).Return(
			[]PartitionReplicas{
				{"kafka-topic-1", 0, []BrokerID{2, 1}},
				{"kafka-topic-1", 1, []BrokerID{1, 2}},
				{"kafka-topic-1", 2, []BrokerID{3, 1}},
			}, nil)

		mockStrategy.On("Name").Return(strategy)

		rebalancer := NewRebalancer(RebalancerConfigs{
			Cluster:    mockCluster,
			Strategies: []Strategy{mockStrategy},
		})

		actual, err := rebalancer.Generate(input)
		exitOnErr(t, err)
		assert.EqualValues(t, expected, actual)
	})
}

func TestRebalancer_distributionDiff(t *testing.T) {
	tests := []struct {
		old      []PartitionReplicas
		new      []PartitionReplicas
		expected []PartitionReplicas
	}{
		{
			old: []PartitionReplicas{
				{"kafka-topic-1", 0, []BrokerID{2, 1}},
				{"kafka-topic-1", 1, []BrokerID{1, 2}},
				{"kafka-topic-2", 0, []BrokerID{1, 1}},
				{"kafka-topic-2", 1, []BrokerID{3, 1}},
			},
			new: []PartitionReplicas{
				{"kafka-topic-1", 0, []BrokerID{1, 3}},
				{"kafka-topic-1", 1, []BrokerID{1, 2}},
				{"kafka-topic-2", 0, []BrokerID{1, 1}},
				{"kafka-topic-2", 1, []BrokerID{3, 1}},
				{"kafka-topic-2", 2, []BrokerID{2, 1}},
			},
			expected: []PartitionReplicas{
				{"kafka-topic-1", 0, []BrokerID{1, 3}},
				{"kafka-topic-2", 2, []BrokerID{2, 1}},
			},
		},
		{
			old: []PartitionReplicas{},
			new: []PartitionReplicas{
				{"kafka-topic-1", 0, []BrokerID{1, 3}},
				{"kafka-topic-1", 1, []BrokerID{1, 2}},
				{"kafka-topic-2", 0, []BrokerID{1, 1}},
			},
			expected: []PartitionReplicas{
				{"kafka-topic-1", 0, []BrokerID{1, 3}},
				{"kafka-topic-1", 1, []BrokerID{1, 2}},
				{"kafka-topic-2", 0, []BrokerID{1, 1}},
			},
		},
		{
			old: []PartitionReplicas{
				{"kafka-topic-1", 0, []BrokerID{2, 1}},
				{"kafka-topic-1", 1, []BrokerID{1, 2}},
				{"kafka-topic-2", 0, []BrokerID{1, 1}},
				{"kafka-topic-2", 1, []BrokerID{3, 1}},
			},
			new: []PartitionReplicas{
				{"kafka-topic-1", 1, []BrokerID{1, 2}},
				{"kafka-topic-2", 1, []BrokerID{3, 1}},
			},
			expected: nil,
		},
		{
			old: []PartitionReplicas{
				{"kafka-topic-1", 0, []BrokerID{2, 1}},
				{"kafka-topic-1", 1, []BrokerID{1, 2}},
				{"kafka-topic-2", 0, []BrokerID{1, 1}},
				{"kafka-topic-2", 1, []BrokerID{3, 1}},
			},
			new: []PartitionReplicas{
				{"kafka-topic-1", 1, []BrokerID{3, 1}},
				{"kafka-topic-2", 1, []BrokerID{3, 2}},
			},
			expected: []PartitionReplicas{
				{"kafka-topic-1", 1, []BrokerID{3, 1}},
				{"kafka-topic-2", 1, []BrokerID{3, 2}},
			},
		},
	}

	for index, test := range tests {
		t.Run(indexedScenario(index), func(t *testing.T) {
			actual := distributionDiff(test.new, test.old)
			assert.EqualValues(t, test.expected, actual)
		})
	}
}

func TestRebalancer_brokers(t *testing.T) {
	mockCluster := &MockClusterAPI{}
	mockCluster.On("Brokers").Return(
		[]Broker{
			{Id: 0, Endpoints: []string{"PLAINTEXT://localhost:19092"}, Rack: "1", Host: "localhost", Timestamp: 1534034254933, Port: 19092, Version: 4},
			{Id: 1, Endpoints: []string{"PLAINTEXT://localhost:29092"}, Rack: "2", Host: "localhost", Timestamp: 1534034254933, Port: 29092, Version: 4},
		}, nil)
	mockCluster.On("Broker", BrokerID(1)).Return(
		Broker{Id: 1, Endpoints: []string{"PLAINTEXT://localhost:29092"}, Rack: "2", Host: "localhost", Timestamp: 1534034254933, Port: 29092, Version: 4},
		nil)

	mockCluster.On("Broker", BrokerID(2)).Return(Broker{}, errors.New("invalid broker"))

	rebalancer := &Rebalancer{
		cluster: mockCluster,
	}

	tests := []struct {
		input    []BrokerID
		expected []Broker
		err      error
	}{
		{
			input: nil,
			expected: []Broker{
				{Id: 0, Endpoints: []string{"PLAINTEXT://localhost:19092"}, Rack: "1", Host: "localhost", Timestamp: 1534034254933, Port: 19092, Version: 4},
				{Id: 1, Endpoints: []string{"PLAINTEXT://localhost:29092"}, Rack: "2", Host: "localhost", Timestamp: 1534034254933, Port: 29092, Version: 4},
			},
		},
		{
			input: []BrokerID{1},
			expected: []Broker{
				{Id: 1, Endpoints: []string{"PLAINTEXT://localhost:29092"}, Rack: "2", Host: "localhost", Timestamp: 1534034254933, Port: 29092, Version: 4},
			},
		},
		{
			input:    []BrokerID{1, 2},
			expected: nil,
			err:      errors.New("invalid broker"),
		},
	}

	for index, test := range tests {
		t.Run(indexedScenario(index), func(t *testing.T) {
			actual, err := rebalancer.brokers(test.input)
			if test.err != nil {
				assert.Error(t, err)
				if err != nil {
					assert.Equal(t, test.err.Error(), err.Error())
				}
				return
			}

			exitOnErr(t, err)
			assert.EqualValues(t, test.expected, actual)
		})
	}
}

func TestRebalancer_Execute(t *testing.T) {
	input := []PartitionReplicas{
		{"kafka-topic-1", 1, []BrokerID{3, 1}},
		{"kafka-topic-2", 1, []BrokerID{3, 2}},
	}

	mockReq := ReassignmentReq{
		Version:    1,
		Partitions: input,
	}

	mockCluster := &MockClusterAPI{}
	mockCluster.On("ReassignPartitions", mockReq).Return(nil)
	mockCluster.On("PartitionReassignRequest", input).Return(mockReq)

	rebalancer := &Rebalancer{
		cluster: mockCluster,
	}

	err := rebalancer.Execute(input)
	assert.NoError(t, err)
}
