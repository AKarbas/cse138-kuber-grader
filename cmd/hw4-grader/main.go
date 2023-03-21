package main

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"

	"github.com/AKarbas/cse138-kuber-grader/internal/kvs4"
)

type Test struct {
	Run         TestFunc
	Description string
	MaxScore    int
	Weight      int
}

type TestFunc func() int

func main() {
	groupName := os.Getenv("GROUP")
	if groupName == "" {
		fmt.Println("failed: expected group name in environment variable GROUP")
		os.Exit(1)
	}
	log := logrus.New().WithFields(logrus.Fields{
		"group": groupName,
	})
	conf := kvs4.TestConfig{
		Registry:  "localhost:32000",
		GroupName: groupName,
		ImageTag:  "cse138-hw4-v1.0",
		Namespace: "default",
	}

	log.Info("multiple tests are executed with different weights.")
	log.Info("when a test logs an Error it fail-stops, but when a test logs a Warning the test continues (but " +
		"you don't get the points for the part where the warning was logged.")
	log.Info("all data operations were done with a time-out of >=20 seconds; and \"context deadline exceeded\" " +
		"means longer waits.")
	log.Info("all tests that expect a non-500 status code were done after waiting for the eventual consistency " +
		"period, or the partition that receives the request has the entire causal history of the request.")
	log.Info("after each view change or network heal, your system had >=10 seconds to ensure consistency.")

	var tests []Test

	for s := 1; s <= 4; s++ {
		s := s
		tests = append(tests, Test{
			Run:         func() int { return kvs4.BasicKvTest(conf, kvs4.ViewConfig{NumNodes: 4, NumShards: s}) },
			Description: fmt.Sprintf("basicKV test with 4 nodes and %d shard(s)", s),
			MaxScore:    kvs4.BasicKVMaxScore,
			Weight:      1,
		})
	}

	for s := 2; s <= 4; s++ {
		s := s
		tests = append(tests, Test{
			Run:         func() int { return kvs4.AvailabilityTest(conf, kvs4.ViewConfig{NumNodes: 12, NumShards: s}) },
			Description: fmt.Sprintf("availability test with 12 nodes and %d shards", s),
			MaxScore:    kvs4.AvailabilityMaxScore,
			Weight:      2,
		})
	}

	scores := make([]int, len(tests))
	for idx, t := range tests {
		log.Infof("starting test %d: %s", idx+1, t.Description)
		scores[idx] = t.Run()
		log.Infof("finished test %d with score %d/%d", idx+1, scores[idx], t.MaxScore)
	}

}
