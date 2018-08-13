package kafkactl

import (
	"fmt"
	"testing"
)

func exitOnErr(t *testing.T, err error) {
	if err != nil {
		t.Fatal("[test failed] unexpected error - ", err)
	}
}

func indexedScenario(i int) string {
	return fmt.Sprintf("scenario_%d", i)
}
