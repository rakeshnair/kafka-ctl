package kafkactl

import (
	"fmt"
	"os"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func PrettyPrintPartitionDistribution(pds []TopicBrokerDistribution) {
	tw := tablewriter.NewWriter(os.Stdout)
	tw.SetHeader([]string{"BrokerID", "Leaders", "Replicas"})

	for _, pd := range pds {
		var row []string
		row = append(row, fmt.Sprintf("%d", pd.ID))
		row = append(row, strings.Trim(strings.Join(strings.Fields(fmt.Sprint(pd.Leaders)), ","), "[]"))
		row = append(row, strings.Trim(strings.Join(strings.Fields(fmt.Sprint(pd.Replicas)), ","), "[]"))
		tw.Append(row)
	}

	tw.Render()
}

func PrettyPrintTopicPartitionInfo(tps []TopicPartitionInfo) {
	tw := tablewriter.NewWriter(os.Stdout)
	tw.SetHeader([]string{"Partition", "Replicas", "ISRs"})

	for _, tp := range tps {
		var row []string
		row = append(row, fmt.Sprintf("%s", topicPartition{tp.Topic, tp.Partition}.ToString()))
		row = append(row, strings.Trim(strings.Join(strings.Fields(fmt.Sprint(tp.Replicas)), ","), "[]"))
		row = append(row, strings.Trim(strings.Join(strings.Fields(fmt.Sprint(tp.ISR)), ","), "[]"))
		tw.Append(row)
	}

	tw.Render()
}

func PrettyPrintPartitionReplicas(prs []PartitionDistribution) {
	tw := tablewriter.NewWriter(os.Stdout)
	tw.SetHeader([]string{"Partition", "Replicas"})

	for _, pr := range prs {
		var row []string
		row = append(row, fmt.Sprintf("%s", topicPartition{pr.Topic, pr.Partition}.ToString()))
		row = append(row, strings.Trim(strings.Join(strings.Fields(fmt.Sprint(pr.Replicas)), ","), "[]"))
		tw.Append(row)
	}

	tw.Render()
}
