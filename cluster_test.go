package kafkactl

import (
	"testing"

	"fmt"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
)

func TestCluster_ID(t *testing.T) {
	tests := []struct {
		data     []byte
		expected string
		hasErr   bool
	}{
		{data: []byte("{\"version\":\"1\",\"id\":\"DK9EwDgaS16XkC8O4eOuzw\"}"), expected: "DK9EwDgaS16XkC8O4eOuzw"},
		{data: []byte("{\"id\":\"DK9EwDgaS16XkC8O4eOuzw\"}"), expected: "DK9EwDgaS16XkC8O4eOuzw"},
		{data: []byte("{\"version\":\"1\"}"), expected: ""},
		{data: []byte(""), expected: "", hasErr: true},
	}

	for i, test := range tests {
		t.Run(indexedScenario(i), func(t *testing.T) {
			defer func() {
				removeBrokerNode(t)
			}()

			c := testCluster(t)
			err := c.store.Set(ClusterIdPath, test.data)
			exitOnErr(t, err)

			actual, err := c.ID()
			if !test.hasErr {
				exitOnErr(t, err)
				assert.Equal(t, test.expected, actual)
				return
			}

			assert.Error(t, err)
		})
	}
}

func TestCluster_Controller(t *testing.T) {
	tests := []struct {
		id         int
		cntrData   string
		brokerData string
		expected   Broker
		hasErr     bool
	}{
		{
			id:         2,
			cntrData:   "{\"version\":1,\"brokerid\":2,\"timestamp\":\"1534034254620\"}",
			brokerData: "{\"endpoints\":[\"PLAINTEXT://localhost:19092\"],\"rack\":\"1\",\"jmx_port\":-1,\"host\":\"localhost\",\"timestamp\":\"1534034254933\",\"port\":19092,\"version\":4}",
			expected:   Broker{Id: 2, Endpoints: []string{"PLAINTEXT://localhost:19092"}, Rack: "1", JmxPort: -1, Host: "localhost", Timestamp: 1534034254933, Port: 19092, Version: 4},
		},
		{
			id:         3,
			cntrData:   "{\"brokerid\":3}",
			brokerData: "{\"endpoints\":[\"PLAINTEXT://localhost:19092\"],\"rack\":\"1\",\"jmx_port\":-1,\"host\":\"localhost\",\"timestamp\":\"1534034254933\",\"port\":19092,\"version\":4}",
			expected:   Broker{Id: 3, Endpoints: []string{"PLAINTEXT://localhost:19092"}, Rack: "1", JmxPort: -1, Host: "localhost", Timestamp: 1534034254933, Port: 19092, Version: 4},
		},
		{
			cntrData: "{\"version\":1}",
			hasErr:   true,
		},
	}

	for i, test := range tests {
		t.Run(indexedScenario(i), func(t *testing.T) {
			defer func() {
				removeBrokerNode(t)
			}()

			c := testCluster(t)
			err := c.store.Set(ControllerPath, []byte(test.cntrData))
			exitOnErr(t, err)
			err = c.store.Set(fmt.Sprintf("%s/%d", BrokerIdsPath, test.id), []byte(test.brokerData))
			exitOnErr(t, err)

			actual, err := c.Controller()
			if !test.hasErr {
				exitOnErr(t, err)
				assert.EqualValues(t, test.expected, actual)
				return
			}

			assert.Error(t, err)
		})
	}
}

func TestCluster_Broker(t *testing.T) {
	tests := []struct {
		input    BrokerID
		seed     []string
		expected Broker
		hasErr   bool
	}{
		{
			input: 0,
			seed: []string{
				// we will be using the index value as brokerID
				"{\"endpoints\":[\"PLAINTEXT://localhost:19092\"],\"rack\":\"1\",\"jmx_port\":-1,\"host\":\"localhost\",\"timestamp\":\"1534034254933\",\"port\":19092,\"version\":4}",
				"{\"endpoints\":[\"PLAINTEXT://localhost:29092\"],\"rack\":\"2\",\"jmx_port\":-1,\"host\":\"localhost\",\"timestamp\":\"1534034254933\",\"port\":29092,\"version\":4}",
			},
			expected: Broker{Id: 0, Endpoints: []string{"PLAINTEXT://localhost:19092"}, Rack: "1", JmxPort: -1, Host: "localhost", Timestamp: 1534034254933, Port: 19092, Version: 4},
		},
		{
			input:  0,
			seed:   []string{},
			hasErr: true,
		},
	}

	for i, test := range tests {
		t.Run(indexedScenario(i), func(t *testing.T) {
			defer func() {
				removeBrokerNode(t)
			}()

			c := testCluster(t)
			for index, seed := range test.seed {
				brkpath := fmt.Sprintf("%s/%d", BrokerIdsPath, index)
				c.store.Set(brkpath, []byte(seed))
			}

			actual, err := c.Broker(test.input)
			if !test.hasErr {
				exitOnErr(t, err)
				assert.EqualValues(t, test.expected, actual)
				return
			}

			assert.Error(t, err)

		})
	}
}

func TestCluster_Brokers(t *testing.T) {
	tests := []struct {
		seed     []string
		expected []Broker
		hasErr   bool
	}{
		{
			seed: []string{
				// we will be using the index value as brokerID
				"{\"endpoints\":[\"PLAINTEXT://localhost:19092\"],\"rack\":\"1\",\"jmx_port\":-1,\"host\":\"localhost\",\"timestamp\":\"1534034254933\",\"port\":19092,\"version\":4}",
				"{\"endpoints\":[\"PLAINTEXT://localhost:29092\"],\"rack\":\"2\",\"jmx_port\":-1,\"host\":\"localhost\",\"timestamp\":\"1534034254933\",\"port\":29092,\"version\":4}",
			},
			expected: []Broker{
				{Id: 0, Endpoints: []string{"PLAINTEXT://localhost:19092"}, Rack: "1", JmxPort: -1, Host: "localhost", Timestamp: 1534034254933, Port: 19092, Version: 4},
				{Id: 1, Endpoints: []string{"PLAINTEXT://localhost:29092"}, Rack: "2", JmxPort: -1, Host: "localhost", Timestamp: 1534034254933, Port: 29092, Version: 4},
			},
		},
		{
			seed:     []string{},
			expected: []Broker{},
		},
	}

	for i, test := range tests {
		t.Run(indexedScenario(i), func(t *testing.T) {
			defer func() {
				removeBrokerNode(t)
			}()

			c := testCluster(t)
			for index, seed := range test.seed {
				brkpath := fmt.Sprintf("%s/%d", BrokerIdsPath, index)
				c.store.Set(brkpath, []byte(seed))
			}

			actual, err := c.Brokers()
			switch {
			case len(test.expected) == 0:
				assert.Equal(t, 0, len(actual))
			default:
				if !test.hasErr {
					exitOnErr(t, err)
					assert.EqualValues(t, test.expected, actual)
					return
				}

				assert.Error(t, err)
			}
		})
	}
}

func TestCluster_Topics(t *testing.T) {
	tests := []struct {
		seed     []string
		expected []string
		hasErr   bool
	}{
		{
			seed:     []string{"topic1", "topic2"},
			expected: []string{"topic1", "topic2"},
		},
		{
			seed:     []string{},
			expected: []string{},
		},
	}

	for i, test := range tests {
		t.Run(indexedScenario(i), func(t *testing.T) {
			defer func() {
				removeBrokerNode(t)
			}()

			c := testCluster(t)
			for _, seed := range test.seed {
				path := fmt.Sprintf("%s/%s", TopicPath, seed)
				c.store.Set(path, []byte(""))
			}

			actual, err := c.Topics()

			switch {
			case len(test.expected) == 0:
				assert.Equal(t, 0, len(actual))
			default:
				if !test.hasErr {
					exitOnErr(t, err)
					assert.EqualValues(t, test.expected, actual)
					return
				}

				assert.Error(t, err)
			}

		})
	}
}

func TestCluster_DescribeTopic(t *testing.T) {
	type kv struct{ k, v string }
	tests := []struct {
		input    string
		seed     []kv
		expected []TopicPartitionInfo
		err      error
	}{
		{
			input: "kafka-topic-1",
			seed: []kv{
				{k: "/brokers/topics/kafka-topic-1", v: "{\"version\":1,\"partitions\":{\"2\":[2,1],\"1\":[1,2],\"0\":[2,1]}}"},
				{k: "/brokers/topics/kafka-topic-1/partitions/0/state", v: "{\"controller_epoch\":49,\"leader\":2,\"version\":1,\"leader_epoch\":0,\"isr\":[2,1]}"},
				{k: "/brokers/topics/kafka-topic-1/partitions/1/state", v: "{\"controller_epoch\":49,\"leader\":1,\"version\":1,\"leader_epoch\":0,\"isr\":[1,2]}"},
				{k: "/brokers/topics/kafka-topic-1/partitions/2/state", v: "{\"controller_epoch\":49,\"leader\":2,\"version\":1,\"leader_epoch\":0,\"isr\":[2,1]}"},
			},
			expected: []TopicPartitionInfo{
				{TopicPartition{"kafka-topic-1", 0}, 2, 2, []BrokerID{2, 1}, []BrokerID{2, 1}},
				{TopicPartition{"kafka-topic-1", 1}, 2, 1, []BrokerID{1, 2}, []BrokerID{1, 2}},
				{TopicPartition{"kafka-topic-1", 2}, 2, 2, []BrokerID{2, 1}, []BrokerID{2, 1}},
			},
		},
		{
			input: "kafka-topic-2",
			seed: []kv{
				{k: "/brokers/topics/kafka-topic-2", v: "{\"version\":1,\"partitions\":{\"0\":[2,1]}}"},
				{k: "/brokers/topics/kafka-topic-2/partitions/0/state", v: "{\"controller_epoch\":49,\"leader\":2,\"version\":1,\"leader_epoch\":0,\"isr\":[2,1]}"},
			},
			expected: []TopicPartitionInfo{
				{TopicPartition{"kafka-topic-2", 0}, 2, 2, []BrokerID{2, 1}, []BrokerID{2, 1}},
			},
		},
		{
			input: "kafka-topic-3",
			seed:  []kv{},
			err:   ErrNoTopic,
		},
	}

	for index, test := range tests {
		t.Run(indexedScenario(index), func(t *testing.T) {
			defer func() {
				removeBrokerNode(t)
			}()

			c := testCluster(t)

			for _, input := range test.seed {
				c.store.Set(input.k, []byte(input.v))
			}

			actual, err := c.DescribeTopic(test.input)

			if test.err == nil {
				exitOnErr(t, err)
				assert.EqualValues(t, test.expected, actual)
				return
			}

			assert.Equal(t, test.err, err)
		})
	}
}

func TestCluster_DescribeAllTopics(t *testing.T) {
	type kv struct{ k, v string }
	tests := []struct {
		seed     []kv
		expected []TopicPartitionInfo
	}{
		{
			seed: []kv{
				{k: "/brokers/topics/kafka-topic-1", v: "{\"version\":1,\"partitions\":{\"2\":[2,1],\"1\":[1,2],\"0\":[2,1]}}"},
				{k: "/brokers/topics/kafka-topic-1/partitions/0/state", v: "{\"controller_epoch\":49,\"leader\":2,\"version\":1,\"leader_epoch\":0,\"isr\":[2,1]}"},
				{k: "/brokers/topics/kafka-topic-1/partitions/1/state", v: "{\"controller_epoch\":49,\"leader\":1,\"version\":1,\"leader_epoch\":0,\"isr\":[1,2]}"},
				{k: "/brokers/topics/kafka-topic-1/partitions/2/state", v: "{\"controller_epoch\":49,\"leader\":2,\"version\":1,\"leader_epoch\":0,\"isr\":[2,1]}"},
			},
			expected: []TopicPartitionInfo{
				{TopicPartition{"kafka-topic-1", 0}, 2, 2, []BrokerID{2, 1}, []BrokerID{2, 1}},
				{TopicPartition{"kafka-topic-1", 1}, 2, 1, []BrokerID{1, 2}, []BrokerID{1, 2}},
				{TopicPartition{"kafka-topic-1", 2}, 2, 2, []BrokerID{2, 1}, []BrokerID{2, 1}},
			},
		},
		{
			seed: []kv{
				{k: "/brokers/topics/kafka-topic-1", v: "{\"version\":1,\"partitions\":{\"2\":[2,1],\"1\":[1,2],\"0\":[2,1]}}"},
				{k: "/brokers/topics/kafka-topic-1/partitions/0/state", v: "{\"controller_epoch\":49,\"leader\":2,\"version\":1,\"leader_epoch\":0,\"isr\":[2,1]}"},
				{k: "/brokers/topics/kafka-topic-1/partitions/1/state", v: "{\"controller_epoch\":49,\"leader\":1,\"version\":1,\"leader_epoch\":0,\"isr\":[1,2]}"},
				{k: "/brokers/topics/kafka-topic-1/partitions/2/state", v: "{\"controller_epoch\":49,\"leader\":2,\"version\":1,\"leader_epoch\":0,\"isr\":[2,1]}"},
				{k: "/brokers/topics/kafka-topic-2", v: "{\"version\":1,\"partitions\":{\"2\":[2,1],\"1\":[1,2],\"0\":[2,1]}}"},
				{k: "/brokers/topics/kafka-topic-2/partitions/0/state", v: "{\"controller_epoch\":49,\"leader\":2,\"version\":1,\"leader_epoch\":0,\"isr\":[2,1]}"},
				{k: "/brokers/topics/kafka-topic-2/partitions/1/state", v: "{\"controller_epoch\":49,\"leader\":1,\"version\":1,\"leader_epoch\":0,\"isr\":[1,2]}"},
				{k: "/brokers/topics/kafka-topic-2/partitions/2/state", v: "{\"controller_epoch\":49,\"leader\":2,\"version\":1,\"leader_epoch\":0,\"isr\":[2,1]}"},
			},
			expected: []TopicPartitionInfo{
				{TopicPartition{"kafka-topic-1", 0}, 2, 2, []BrokerID{2, 1}, []BrokerID{2, 1}},
				{TopicPartition{"kafka-topic-1", 1}, 2, 1, []BrokerID{1, 2}, []BrokerID{1, 2}},
				{TopicPartition{"kafka-topic-1", 2}, 2, 2, []BrokerID{2, 1}, []BrokerID{2, 1}},
				{TopicPartition{"kafka-topic-2", 0}, 2, 2, []BrokerID{2, 1}, []BrokerID{2, 1}},
				{TopicPartition{"kafka-topic-2", 1}, 2, 1, []BrokerID{1, 2}, []BrokerID{1, 2}},
				{TopicPartition{"kafka-topic-2", 2}, 2, 2, []BrokerID{2, 1}, []BrokerID{2, 1}},
			},
		},
		{
			seed: []kv{
				{k: "/brokers/topics/kafka-topic-2", v: "{\"version\":1,\"partitions\":{\"0\":[2,1]}}"},
				{k: "/brokers/topics/kafka-topic-2/partitions/0/state", v: "{\"controller_epoch\":49,\"leader\":2,\"version\":1,\"leader_epoch\":0,\"isr\":[2,1]}"},
			},
			expected: []TopicPartitionInfo{
				{TopicPartition{"kafka-topic-2", 0}, 2, 2, []BrokerID{2, 1}, []BrokerID{2, 1}},
			},
		},
		{
			seed:     []kv{},
			expected: []TopicPartitionInfo{},
		},
	}

	for index, test := range tests {
		t.Run(indexedScenario(index), func(t *testing.T) {
			defer func() {
				removeBrokerNode(t)
			}()

			c := testCluster(t)

			for _, input := range test.seed {
				c.store.Set(input.k, []byte(input.v))
			}

			actual, err := c.DescribeAllTopics()
			exitOnErr(t, err)
			switch {
			case len(test.expected) == 0:
				assert.Equal(t, 0, len(actual))
			default:
				assert.EqualValues(t, test.expected, actual)
			}
		})
	}
}

func TestCluster_DescribeTopicsForBroker(t *testing.T) {
	type kv struct{ k, v string }
	tests := []struct {
		input    BrokerID
		seed     []kv
		expected []TopicPartitionInfo
	}{
		{
			input: 2,
			seed: []kv{
				{k: "/brokers/topics/kafka-topic-1", v: "{\"version\":1,\"partitions\":{\"2\":[2,1],\"1\":[1,2],\"0\":[2,1]}}"},
				{k: "/brokers/topics/kafka-topic-1/partitions/0/state", v: "{\"controller_epoch\":49,\"leader\":2,\"version\":1,\"leader_epoch\":0,\"isr\":[2,1]}"},
				{k: "/brokers/topics/kafka-topic-1/partitions/1/state", v: "{\"controller_epoch\":49,\"leader\":1,\"version\":1,\"leader_epoch\":0,\"isr\":[1,2]}"},
				{k: "/brokers/topics/kafka-topic-1/partitions/2/state", v: "{\"controller_epoch\":49,\"leader\":2,\"version\":1,\"leader_epoch\":0,\"isr\":[2,1]}"},
			},
			expected: []TopicPartitionInfo{
				{TopicPartition{"kafka-topic-1", 0}, 2, 2, []BrokerID{2, 1}, []BrokerID{2, 1}},
				{TopicPartition{"kafka-topic-1", 2}, 2, 2, []BrokerID{2, 1}, []BrokerID{2, 1}},
			},
		},
		{
			input: 2,
			seed: []kv{
				{k: "/brokers/topics/kafka-topic-1", v: "{\"version\":1,\"partitions\":{\"2\":[2,1],\"1\":[1,2],\"0\":[2,1]}}"},
				{k: "/brokers/topics/kafka-topic-1/partitions/0/state", v: "{\"controller_epoch\":49,\"leader\":2,\"version\":1,\"leader_epoch\":0,\"isr\":[2,1]}"},
				{k: "/brokers/topics/kafka-topic-1/partitions/1/state", v: "{\"controller_epoch\":49,\"leader\":1,\"version\":1,\"leader_epoch\":0,\"isr\":[1,2]}"},
				{k: "/brokers/topics/kafka-topic-1/partitions/2/state", v: "{\"controller_epoch\":49,\"leader\":2,\"version\":1,\"leader_epoch\":0,\"isr\":[2,1]}"},
				{k: "/brokers/topics/kafka-topic-2", v: "{\"version\":1,\"partitions\":{\"2\":[2,1],\"1\":[1,2],\"0\":[2,1]}}"},
				{k: "/brokers/topics/kafka-topic-2/partitions/0/state", v: "{\"controller_epoch\":49,\"leader\":2,\"version\":1,\"leader_epoch\":0,\"isr\":[2,1]}"},
				{k: "/brokers/topics/kafka-topic-2/partitions/1/state", v: "{\"controller_epoch\":49,\"leader\":1,\"version\":1,\"leader_epoch\":0,\"isr\":[1,2]}"},
				{k: "/brokers/topics/kafka-topic-2/partitions/2/state", v: "{\"controller_epoch\":49,\"leader\":2,\"version\":1,\"leader_epoch\":0,\"isr\":[2,1]}"},
			},
			expected: []TopicPartitionInfo{
				{TopicPartition{"kafka-topic-1", 0}, 2, 2, []BrokerID{2, 1}, []BrokerID{2, 1}},
				{TopicPartition{"kafka-topic-1", 2}, 2, 2, []BrokerID{2, 1}, []BrokerID{2, 1}},
				{TopicPartition{"kafka-topic-2", 0}, 2, 2, []BrokerID{2, 1}, []BrokerID{2, 1}},
				{TopicPartition{"kafka-topic-2", 2}, 2, 2, []BrokerID{2, 1}, []BrokerID{2, 1}},
			},
		},
		{
			input: 2,
			seed: []kv{
				{k: "/brokers/topics/kafka-topic-2", v: "{\"version\":1,\"partitions\":{\"0\":[2,1]}}"},
				{k: "/brokers/topics/kafka-topic-2/partitions/0/state", v: "{\"controller_epoch\":49,\"leader\":2,\"version\":1,\"leader_epoch\":0,\"isr\":[2,1]}"},
			},
			expected: []TopicPartitionInfo{
				{TopicPartition{"kafka-topic-2", 0}, 2, 2, []BrokerID{2, 1}, []BrokerID{2, 1}},
			},
		},
		{
			seed:     []kv{},
			expected: []TopicPartitionInfo{},
		},
	}

	for index, test := range tests {
		t.Run(indexedScenario(index), func(t *testing.T) {
			defer func() {
				removeBrokerNode(t)
			}()

			c := testCluster(t)

			for _, input := range test.seed {
				c.store.Set(input.k, []byte(input.v))
			}

			actual, err := c.DescribeTopicsForBroker(test.input)
			exitOnErr(t, err)
			switch {
			case len(test.expected) == 0:
				assert.Equal(t, 0, len(actual))
			default:
				assert.EqualValues(t, test.expected, actual)
			}
		})
	}
}

func TestCluster_PartitionDistribution(t *testing.T) {
	type kv struct{ k, v string }
	tests := []struct {
		input    string
		seed     []kv
		expected []TopicBrokerDistribution
		err      error
	}{
		{
			input: "kafka-topic-1",
			seed: []kv{
				{k: "/brokers/ids/1", v: "{\"endpoints\":[\"PLAINTEXT://localhost:19092\"],\"rack\":\"1\",\"jmx_port\":-1,\"host\":\"localhost\",\"timestamp\":\"1534034254933\",\"port\":19092,\"version\":4}"},
				{k: "/brokers/ids/2", v: "{\"endpoints\":[\"PLAINTEXT://localhost:29092\"],\"rack\":\"1\",\"jmx_port\":-1,\"host\":\"localhost\",\"timestamp\":\"1534034254933\",\"port\":29092,\"version\":4}"},
				{k: "/brokers/topics/kafka-topic-1", v: "{\"version\":1,\"partitions\":{\"2\":[2,1],\"1\":[1,2],\"0\":[2,1]}}"},
				{k: "/brokers/topics/kafka-topic-1/partitions/0/state", v: "{\"controller_epoch\":49,\"leader\":2,\"version\":1,\"leader_epoch\":0,\"isr\":[2,1]}"},
				{k: "/brokers/topics/kafka-topic-1/partitions/1/state", v: "{\"controller_epoch\":49,\"leader\":1,\"version\":1,\"leader_epoch\":0,\"isr\":[1,2]}"},
				{k: "/brokers/topics/kafka-topic-1/partitions/2/state", v: "{\"controller_epoch\":49,\"leader\":2,\"version\":1,\"leader_epoch\":0,\"isr\":[2,1]}"},
			},
			expected: []TopicBrokerDistribution{
				{"kafka-topic-1", 1, []int64{1}, []int64{0, 2}},
				{"kafka-topic-1", 2, []int64{0, 2}, []int64{1}},
			},
		},
		{
			input: "kafka-topic-1",
			seed: []kv{
				{k: "/brokers/ids/1", v: "{\"endpoints\":[\"PLAINTEXT://localhost:19092\"],\"rack\":\"1\",\"jmx_port\":-1,\"host\":\"localhost\",\"timestamp\":\"1534034254933\",\"port\":19092,\"version\":4}"},
				{k: "/brokers/ids/2", v: "{\"endpoints\":[\"PLAINTEXT://localhost:29092\"],\"rack\":\"1\",\"jmx_port\":-1,\"host\":\"localhost\",\"timestamp\":\"1534034254933\",\"port\":29092,\"version\":4}"},
			},
			err: ErrNoTopic,
		},
		{
			input: "kafka-topic-1",
			seed:  []kv{},
			err:   zk.ErrNoNode,
		},
	}

	for index, test := range tests {
		t.Run(indexedScenario(index), func(t *testing.T) {
			defer func() {
				removeBrokerNode(t)
			}()

			c := testCluster(t)

			for _, input := range test.seed {
				c.store.Set(input.k, []byte(input.v))
			}

			actual, err := c.PartitionDistribution(test.input)
			if test.err != nil {
				assert.Equal(t, test.err, err)
				return
			}

			exitOnErr(t, err)
			assert.EqualValues(t, test.expected, actual)
		})
	}
}

func TestCluster_PartitionReplicaDistribution(t *testing.T) {
	type kv struct{ k, v string }
	tests := []struct {
		input    string
		seed     []kv
		expected []PartitionReplicas
		err      error
	}{
		{
			input: "kafka-topic-1",
			seed: []kv{
				{k: "/brokers/topics/kafka-topic-1", v: "{\"version\":1,\"partitions\":{\"2\":[2,1],\"1\":[1,2],\"0\":[2,1]}}"},
				{k: "/brokers/topics/kafka-topic-1/partitions/0/state", v: "{\"controller_epoch\":49,\"leader\":2,\"version\":1,\"leader_epoch\":0,\"isr\":[2,1]}"},
				{k: "/brokers/topics/kafka-topic-1/partitions/1/state", v: "{\"controller_epoch\":49,\"leader\":1,\"version\":1,\"leader_epoch\":0,\"isr\":[1,2]}"},
				{k: "/brokers/topics/kafka-topic-1/partitions/2/state", v: "{\"controller_epoch\":49,\"leader\":2,\"version\":1,\"leader_epoch\":0,\"isr\":[2,1]}"},
			},
			expected: []PartitionReplicas{
				{"kafka-topic-1", 0, []BrokerID{2, 1}},
				{"kafka-topic-1", 1, []BrokerID{1, 2}},
				{"kafka-topic-1", 2, []BrokerID{2, 1}},
			},
		},
		{
			input: "kafka-topic-2",
			seed: []kv{
				{k: "/brokers/topics/kafka-topic-2", v: "{\"version\":1,\"partitions\":{\"0\":[2,1]}}"},
				{k: "/brokers/topics/kafka-topic-2/partitions/0/state", v: "{\"controller_epoch\":49,\"leader\":2,\"version\":1,\"leader_epoch\":0,\"isr\":[2,1]}"},
			},
			expected: []PartitionReplicas{
				{"kafka-topic-2", 0, []BrokerID{2, 1}},
			},
		},
		{
			input: "kafka-topic-3",
			seed:  []kv{},
			err:   ErrNoTopic,
		},
	}

	for index, test := range tests {
		t.Run(indexedScenario(index), func(t *testing.T) {
			defer func() {
				removeBrokerNode(t)
			}()

			c := testCluster(t)

			for _, input := range test.seed {
				c.store.Set(input.k, []byte(input.v))
			}

			actual, err := c.PartitionReplicaDistribution(test.input)

			if test.err == nil {
				exitOnErr(t, err)
				assert.EqualValues(t, test.expected, actual)
				return
			}

			assert.Equal(t, test.err, err)
		})
	}
}

func testCluster(t *testing.T) *Cluster { return NewCluster(&ZkClusterStore{conn: testZkConn(t)}) }

func testStore(t *testing.T) *ZkClusterStore { return &ZkClusterStore{conn: testZkConn(t)} }

func removeBrokerNode(t *testing.T) {
	store := testStore(t)
	store.Remove("/brokers")
}
