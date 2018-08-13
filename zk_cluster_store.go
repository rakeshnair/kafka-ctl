package kafkactl

import (
	"time"

	"strings"

	"sort"

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
//  List("/brokers") will return {"1", "2"}
func (zks *ZkClusterStore) List(path string) ([]string, error) {
	children, _, err := zks.conn.Children(path)
	if err != nil {
		return []string{}, err
	}
	sort.Strings(children)
	return children, nil
}

// Get returns the data stored in the path
func (zks *ZkClusterStore) Get(path string) ([]byte, error) {
	out, _, err := zks.conn.Get(path)
	return out, err
}

// Set stores in the input data bytes into the path
func (zks *ZkClusterStore) Set(path string, data []byte) error {
	// todo: make ACL's configurable

	// If path exists, simply update with a new version number
	exists, _, err := zks.conn.Exists(path)
	switch {
	case err != nil:
		return err
	case exists:
		_, err := zks.conn.Set(path, data, -1)
		return err
	}

	// Path does not exist. Ensure all the parent nodes are created
	err = zks.buildPrefixes(prefix(path))
	if err != nil {
		return err
	}

	// Create the final leaf node and update with input data
	_, err = zks.conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
	return err
}

// Close terminates connection to the underlying store and performs necessary clean up
func (zks *ZkClusterStore) Close() error {
	zks.conn.Close()
	return nil
}

// Remove deletes the node referenced by the input path and all the child nodes under it
func (zks *ZkClusterStore) Remove(path string) error {
	exists, _, err := zks.conn.Exists(path)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	err = zks.conn.Delete(path, -1)
	if err != nil {
		switch err {
		case zk.ErrNotEmpty:
			ids, err := zks.List(path)
			if err != nil {
				return err
			}

			// delete all the child nodes
			for _, id := range ids {
				path := fmt.Sprintf("%s/%s", path, id)
				err := zks.Remove(path)
				if err != nil {
					return err
				}
			}

			// attempt deleting the current node again after all the child
			// nodes have been deleted
			err = zks.conn.Delete(path, -1)
			if err != nil {
				return err
			}
		default:
			return err
		}
	}
	return nil
}

func (zks *ZkClusterStore) buildPrefixes(path string) error {
	if len(path) == 0 {
		return nil
	}

	exists, _, err := zks.conn.Exists(path)
	switch {
	case err != nil:
		return err
	case exists:
		return nil
	}

	err = zks.buildPrefixes(prefix(path))
	if err != nil {
		return err
	}

	_, err = zks.conn.Create(path, []byte(""), 0, zk.WorldACL(zk.PermAll))
	return nil
}

func prefix(path string) string {
	if len(path) == 0 {
		return ""
	}
	return path[:strings.LastIndex(path, "/")]
}
