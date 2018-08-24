package kafkactl

import "time"

// ClusterIDRequest is the request payload for obtaining the id of a given input cluster
type ClusterIDRequest struct {
	Name string `json:"name"`
}

// ClusterIDResponse is the response payload for obtaining the id of a given input cluster
type ClusterIDResponse struct {
	ID string `json:"id"`
}

// ClusterControllerRequest is the request payload for obtaining the info about the broker
// acting as the controller for the given input cluster
type ClusterControllerRequest struct {
	Name string `json:"name"`
}

// ClusterControllerResponse is the response payload for obtaining the info about the broker
// acting as the controller for the given input cluster
type ClusterControllerResponse struct {
	Controller Broker `json:"controller"`
}

// ClusterBrokersRequest is the request payload for obtaining information about all the brokers
// in the input cluster
type ClusterBrokersRequest struct {
	Name string `json:"name"`
}

// ClusterBrokersResponse is the response payload for obtaining information about all the brokers
// in the input cluster
type ClusterBrokersResponse struct {
	Brokers []Broker `json:"brokers"`
}

// ClusterBrokerRequest is the request payload for obtaining information about a specific broker
// in the input cluster
type ClusterBrokerRequest struct {
	Name string   `json:"name"`
	ID   BrokerID `json:"id"`
}

// ClusterBrokerResponse is the response payload for obtaining information about a specific broker
// in the input cluster
type ClusterBrokerResponse struct {
	Broker Broker `json:"broker"`
}

// ClusterTopicsRequest is the request payload for obtaining all the topics in the input cluster
type ClusterTopicsRequest struct {
	Name string `json:"name"`
}

// ClusterTopicsResponse is the response payload for obtaining all the topics in the input cluster
type ClusterTopicsResponse struct {
	Topics []string `json:"topics"`
}

// ClusterDescribeTopicRequest is the request payload for obtaining detailed information about a
// specific topic in the input cluster
type ClusterDescribeTopicRequest struct {
	Name  string `json:"name"`
	Topic string `json:"topic"`
}

// ClusterDescribeTopicResponse is the response payload for obtaining detailed information about a
// specific topic in the input cluster
type ClusterDescribeTopicResponse struct {
	Partitions []TopicPartitionInfo `json:"partitions"`
}

// ClusterDescribeTopicsForBrokerRequest is the request payload for obtaining detailed information
// about all the topics that have leaders on the input BrokerID
type ClusterDescribeTopicsForBrokerRequest struct {
	Name     string   `json:"name"`
	BrokerID BrokerID `json:"broker_id"`
}

// ClusterDescribeTopicsForBrokerResponse is the response payload for obtaining detailed information
// about all the topics that have leaders on the input BrokerID
type ClusterDescribeTopicsForBrokerResponse struct {
	Partitions []TopicPartitionInfo `json:"partitions"`
}

// ClusterReplicaDistributionRequest is the request payload to obtain the
// replica distribution for all the partitions in the topic
type ClusterReplicaDistributionRequest struct {
	Name  string `json:"name"`
	Topic string `json:"topic"`
}

// ClusterReplicaDistributionResponse is the response payload to obtain the
// replica distribution for all the partitions in the topic
type ClusterReplicaDistributionResponse struct {
	Replicas []PartitionReplicas `json:"replicas"`
}

// PingRequest is the request payload to ping the control service
type PingRequest struct {
	Seed string `json:"seed"`
}
type PingResponse struct {
	Seed string    `json:"seed"`
	Time time.Time `json:"time"`
}
