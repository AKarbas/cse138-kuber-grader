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
			Description: fmt.Sprintf("(basicKV test with 4 nodes and %d shard(s) (weight=2)", s),
			MaxScore:    kvs4.BasicKVMaxScore,
			Weight:      2,
		})
	}

	for s := 2; s <= 4; s++ {
		s := s
		tests = append(tests, Test{
			Run:         func() int { return kvs4.AvailabilityTest(conf, kvs4.ViewConfig{NumNodes: 12, NumShards: s}) },
			Description: fmt.Sprintf("availability test with 12 nodes and %d shards (weight=4)", s),
			MaxScore:    kvs4.AvailabilityMaxScore,
			Weight:      4,
		})
	}

	viewConfigPairs := [][2]kvs4.ViewConfig{
		{kvs4.ViewConfig{NumNodes: 8, NumShards: 3}, kvs4.ViewConfig{NumNodes: 9, NumShards: 4}},
		{kvs4.ViewConfig{NumNodes: 8, NumShards: 3}, kvs4.ViewConfig{NumNodes: 8, NumShards: 4}},
		{kvs4.ViewConfig{NumNodes: 8, NumShards: 7}, kvs4.ViewConfig{NumNodes: 15, NumShards: 7}},
		{kvs4.ViewConfig{NumNodes: 8, NumShards: 3}, kvs4.ViewConfig{NumNodes: 2, NumShards: 1}},
	}

	for _, vcPair := range viewConfigPairs {
		vcPair := vcPair
		tests = append(tests, Test{
			Run:         func() int { return kvs4.ViewChangeTest(conf, vcPair[0], vcPair[1], false) },
			Description: fmt.Sprintf("viewChange test (killNodes=false) (weight=3)"),
			MaxScore:    kvs4.ViewChangeMaxScore,
			Weight:      3,
		})
	}

	for _, vcPair := range viewConfigPairs {
		vcPair := vcPair
		tests = append(tests, Test{
			Run:         func() int { return kvs4.ViewChangeTest(conf, vcPair[0], vcPair[1], true) },
			Description: fmt.Sprintf("viewChange test (killNodes=true) (weight=4)"),
			MaxScore:    kvs4.ViewChangeMaxScore,
			Weight:      4,
		})
	}

	extraCredit := 0
	for n1 := 5; n1 <= 7; n1++ {
		n1 := n1
		tests = append(tests, Test{
			Run:         func() int { return kvs4.KeyDistTest(conf, n1) },
			Description: fmt.Sprintf("keyDistribution test with n1=%d, n2=%d (weight=3, extraCredit=1)", n1, n1+1),
			MaxScore:    kvs4.KeyDistMaxScore,
			Weight:      3,
		})
		extraCredit += 1
	}

	scores := make([]int, len(tests))
	for idx, t := range tests {
		log.Infof("starting test %d: %s", idx+1, t.Description)
		scores[idx] = t.Run()
		log.Infof("finished test %d with score %d/%d", idx+1, scores[idx], t.MaxScore)
		if scores[idx] < t.MaxScore {
			log.Warnf("test %d did not finish with full score", idx+1)
		}
	}

	log.Info("all tests done, printing scores again")

	sum := 0.0
	sumWeights := 0.0
	for idx, score := range scores {
		log.Infof("test %d: score=%d/%d, weight=%d", idx+1, score, tests[idx].MaxScore, tests[idx].Weight)
		sum += float64(score) / float64(tests[idx].MaxScore) * float64(tests[idx].Weight)
		sumWeights += float64(tests[idx].Weight)
	}
	sumWeights -= float64(extraCredit)
	res := sum / sumWeights

	log.Infof("Final score overall: %.1f/10", res*10.0)
}
