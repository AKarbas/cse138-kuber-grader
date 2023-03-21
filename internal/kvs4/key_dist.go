package kvs4

import "github.com/sirupsen/logrus"

const KeyDistMaxScore = 0

func KeyDistTest(c TestConfig, n1 int) int {
	log := logrus.New().WithFields(logrus.Fields{
		"test":  "keyDistribution",
		"group": c.GroupName,
	})
	v1 := ViewConfig{NumNodes: n1, NumShards: n1}
	v2 := ViewConfig{NumNodes: n1 + 1, NumShards: n1 + 1}
	log.WithFields(logrus.Fields{
		"viewConfig1": v1.String(),
		"viewConfig2": v2.String(),
	}).Info(
		"starting test. Steps: " +
			"1. create all needed nodes; " +
			"2. put viewConfig1; " +
			"3. get view from all nodes and expect consistency; " +
			"4. do 20K causally independent writes sprayed across all nodes; " +
			"5. sleep for 11 seconds; " +
			"6. expect number of keys in each shard to be within 15% of 20K/s1; " +
			"7. put viewConfig2; " +
			"8. expect number of keys moved to be within 15% of 20K/s2. " +
			"Steps 4, 6, 8 each have 10 points for a total of 30 (step 8 is extra credit).",
	)

	return 0
}
