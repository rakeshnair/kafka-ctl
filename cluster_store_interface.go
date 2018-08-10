package kafkactl

type ClusterStoreAPI interface {
	List(string) ([]string, error)
	ListAll(string) ([]string, error)
	Get(string) ([]byte, error)
	Set(string, []byte) error
}
