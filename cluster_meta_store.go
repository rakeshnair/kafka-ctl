package kafkactl

import (
	"time"

	"fmt"

	"github.com/samuel/go-zookeeper/zk"
)

type ClusterMetaStore interface {
	List(string) ([]string, error)
	ListAll(string) ([]string, error)
	Get(string) ([]byte, error)
}

type ZkClusterMetaStore struct {
	conn *zk.Conn
}

func NewZkClusterMetaStore(url string, timeout time.Duration) (ClusterMetaStore, error) {
	conn, _, err := zk.Connect([]string{url}, timeout)
	if err != nil {
		return nil, err
	}

	return &ZkClusterMetaStore{conn: conn}, nil
}

func (zks *ZkClusterMetaStore) List(path string) ([]string, error) {
	children, _, err := zks.conn.Children(path)
	if err != nil {
		return []string{}, err
	}
	return children, nil
}

func (zks *ZkClusterMetaStore) ListAll(path string) ([]string, error) {
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

func (zks *ZkClusterMetaStore) Get(path string) ([]byte, error) {
	out, _, err := zks.conn.Get(path)
	return out, err
}
