package kafkactl

import (
	"sort"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
)

var localZkEndpoint = "localhost:2181"

func TestZkClusterStore_Set(t *testing.T) {
	tests := []struct {
		inputPath string
		inputData string
		expected  string
	}{
		{"/test", "test data", "test data"},
		{"/test/topics/id1", "test topics id1 data", "test topics id1 data"},
	}

	for i, test := range tests {
		t.Run(indexedScenario(i), func(t *testing.T) {
			conn := testZkConn(t)
			store := &ZkClusterStore{conn: conn}

			err := store.Set(test.inputPath, []byte(test.inputData))
			exitOnErr(t, err)

			actual, _, err := conn.Get(test.inputPath)
			exitOnErr(t, err)

			assert.Equal(t, test.expected, string(actual))
			conn.Delete(test.inputPath, -1)
			conn.Close()
		})
	}
}

func TestZkClusterStore_Get(t *testing.T) {
	tests := []struct {
		input    string
		seed     string
		expected string
	}{
		{"/test", "test data", "test data"},
		{"/test/topics/id1", "test topics id1 data", "test topics id1 data"},
	}

	for i, test := range tests {
		t.Run(indexedScenario(i), func(t *testing.T) {
			conn := testZkConn(t)
			store := &ZkClusterStore{conn: conn}

			err := store.Set(test.input, []byte(test.seed))
			exitOnErr(t, err)

			actual, err := store.Get(test.input)
			exitOnErr(t, err)
			assert.Equal(t, test.expected, string(actual))
			conn.Delete(test.input, -1)
			conn.Close()
		})
	}
}

func TestZkClusterStore_List(t *testing.T) {
	tests := []struct {
		seed     []string
		input    string
		expected []string
	}{
		{[]string{"/broker/id1", "/broker/id2"}, "/broker", []string{"id1", "id2"}},
		{[]string{"/broker/id1"}, "/broker", []string{"id1"}},
		{[]string{"/broker"}, "/broker", []string{}},
	}
	for i, test := range tests {
		t.Run(indexedScenario(i), func(t *testing.T) {
			conn := testZkConn(t)
			store := &ZkClusterStore{conn: conn}

			for _, p := range test.seed {
				err := store.Set(p, []byte(""))
				exitOnErr(t, err)
			}

			actual, err := store.List(test.input)
			exitOnErr(t, err)
			sort.Strings(test.expected)
			assert.EqualValues(t, test.expected, actual)

			for _, p := range test.seed {
				conn.Delete(p, -1)
			}
		})
	}
}

func testZkConn(t *testing.T) *zk.Conn {
	conn, _, err := zk.Connect([]string{localZkEndpoint}, time.Second)
	exitOnErr(t, err)
	return conn
}
