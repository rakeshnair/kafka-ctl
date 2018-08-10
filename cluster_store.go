package kafkactl

import (
	"time"

	"fmt"

	"github.com/samuel/go-zookeeper/zk"
)

// ZkClusterStore implements a ClusterStoreAPI and uses a Zookeeper as the backing store
type ZkClusterStore struct {
	conn *zk.Conn
}

// NewZkClusterStore returns a new ZkClusterStore object
func NewZkClusterStore(url string, timeout time.Duration) (ClusterStoreAPI, error) {
	conn, _, err := zk.Connect([]string{url}, timeout)
	if err != nil {
		return nil, err
	}

	return &ZkClusterStore{conn: conn}, nil
}

// List returns the nodes immediately under a given path
// For eg:
// /brokers
//	|- 1
//	|- 2
//
//  List("/brokers") will return {"/brokers/1", "/brokers/2"}
func (zks *ZkClusterStore) List(path string) ([]string, error) {
	children, _, err := zks.conn.Children(path)
	if err != nil {
		return []string{}, err
	}
	return children, nil
}

// ListAll recursively navigates all the nodes under a given path
// For eg:
// /brokers
//	|- topics
//		|- topic1
//		|- topic2
//
//  List("/brokers") will return {"/brokers/topics", "/brokers/topics/topic1", "/brokers/topics/topic2"}
func (zks *ZkClusterStore) ListAll(path string) ([]string, error) {
	var allNodes []string
	nodes, err := zks.List(path)
	if err != nil {
		return []string{}, err
	}

	for _, node := range nodes {
		absPath := fmt.Sprintf("%s/%s", path, node)
		allNodes = append(allNodes, absPath)
		out, err := zks.ListAll(absPath)
		if err != nil {
			return []string{}, err
		}
		allNodes = append(allNodes, out...)
	}
	return allNodes, nil
}

// Get returns the data stored in the path
func (zks *ZkClusterStore) Get(path string) ([]byte, error) {
	out, _, err := zks.conn.Get(path)
	return out, err
}

// Set stores in the input data bytes into the path
func (zks *ZkClusterStore) Set(path string, data []byte) error {
	// todo: make ACL's configurable
	_, err := zks.conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
	return err
}
