package main

import (
	"os"

	"github.com/sirupsen/logrus"

	"github.com/AKarbas/cse138-kuber-grader/internal/kvs3"
)

func main() {
	groupName := os.Getenv("GROUP")
	if groupName == "" {
		os.Exit(1)
	}
	log := logrus.New().WithFields(logrus.Fields{
		"group": groupName,
	})
	log.Info("Graded using github.com/AKarbas/cse138-kuber-grader")
	log.Info("All tests that expect a non-500 status code were done after waiting for the eventual consistency period, " +
		"or the partition that receives the request has the entire causal history of the request.")
	log.Info("After each view change or network heal, your system had 11 seconds to ensure consistency.")
	log.Info("All data operations were done with a time-out of 21 seconds, and \"context deadline exceeded\" means longer waits.")
	twoNodePerBatch := kvs3.TestConfig{
		Registry:  "localhost:32000",
		ImageTag:  "cse138-hw3-v1.0",
		Namespace: "default",
		GroupName: groupName,
		NumNodes:  2,
		NumKeys:   10,
	}
	threeNodePerBatch := kvs3.TestConfig{
		Registry:  "localhost:32000",
		ImageTag:  "cse138-hw3-v1.0",
		Namespace: "default",
		GroupName: groupName,
		NumNodes:  3,
		NumKeys:   10,
	}

	extraCredit := 1
	scores := make([]int, 5)
	maxes := []int{
		kvs3.BasicKVMaxScore,
		kvs3.PartitionedTotalOrderMaxScore,
		kvs3.BasicViewChangeMaxScore,
		kvs3.PartitionedViewChangeMaxScore,
		kvs3.AvailabilityMaxScore,
	}
	weights := []int{3, 3, 3, 1, 3}
	tests := []kvs3.TestFunc{
		kvs3.BasicKVTest,
		kvs3.PartitionedTotalOrderTest,
		kvs3.BasicViewChangeTest,
		kvs3.PartitionedViewChangeTest,
		kvs3.AvailabilityTest,
	}
	configs := []kvs3.TestConfig{
		threeNodePerBatch,
		twoNodePerBatch,
		twoNodePerBatch,
		twoNodePerBatch,
		threeNodePerBatch,
	}

	for idx, testFunc := range tests {
		log.Infof("Starting test %d", idx+1)
		scores[idx] = testFunc(configs[idx])
		if scores[idx] < maxes[idx] {
			log.WithFields(logrus.Fields{
				"expected": maxes[idx],
				"got":      scores[idx],
			}).Warnf("test %d did not finish with the full score.", idx+1)
		}
	}

	sum := 0.0
	sumWeights := 0.0
	for idx, score := range scores {
		sum += float64(score) / float64(maxes[idx]) * float64(weights[idx])
		sumWeights += float64(weights[idx])
	}
	sumWeights -= float64(extraCredit)
	res := sum / sumWeights

	log.Infof("Final score overall: %.2f", res)
}
