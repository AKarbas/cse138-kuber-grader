package kvs4

import (
	"math"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/AKarbas/cse138-kuber-grader/pkg/k8s"
	"github.com/AKarbas/cse138-kuber-grader/pkg/kvs4client"
)

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
				"8. get view from all nodes and expect consistency; " +
				"9. expect number of keys moved to be within 15% of 20K/s2. " +
				"Steps 4, 6, 9 each have 10 points for a total of 30 (step 9 is extra credit).",
	)

	k8sClient := k8s.Client{}
	score := 0
	defer func(s *int) {
		log.WithField("finalScore", *s).Info("test completed.")
	}(&score)

	if err := PreTestCleanup(k8sClient, c.Namespace, c.GroupName); err != nil {
		log.Errorf("pre-test cleanup faild: %v", err)
		return score
	}

	numNodes := v2.NumNodes
	if err := k8sClient.CreatePods(c.Namespace, c.GroupName, c.Image(), 1, numNodes); err != nil {
		log.Errorf("test start failed; failed to create pods: %v", err)
		return score
	}
	defer PostTestCleanup(k8sClient, c.Namespace, c.GroupName)

	log.Info("nodes created, sleeping for 10s (to let nodes start up)")
	time.Sleep(10 * time.Second)

	// PUT view 1
	allAddrs, err := k8sClient.ListPodAddresses(c.Namespace, k8s.GroupLabels(c.GroupName))
	if err != nil {
		log.Errorf("test start failed; failed to list pod addresses: %v", err)
		return score
	}
	log.Infof("putting view 1 to the nodes (%s)", v1.String())
	view1Addrs := allAddrs[:v1.NumNodes]
	statusCode, err := kvs4client.PutView(view1Addrs[v1.NumNodes-1], kvs4client.ViewReq{Nodes: view1Addrs, NumShards: v1.NumShards})
	if err != nil {
		log.Errorf("failed to put view: %v", err)
		return score
	}
	if statusCode != 200 {
		log.WithFields(logrus.Fields{
			"expected": 200,
			"received": statusCode,
		}).Error("bad status code for put view")
		return score
	}
	log.Info("put view 1 successful")

	// GET view 1
	log.Info("getting views from nodes and checking consistency")
	var view1 kvs4client.ViewResp
	if view1, err = TestViewsConsistent(view1Addrs, v1); err != nil {
		log.Errorf("test failed: %v", err)
		return score
	}
	log.Info("get view from all nodes successful and all views consistent")

	// Independent Puts
	sprayConf := SprayConfig{
		addresses:           view1Addrs,
		minI:                1,
		maxI:                20000,
		minJ:                1,
		maxJ:                1,
		cm:                  nil,
		noCm:                true,
		acceptedStatusCodes: []int{200, 201},
	}
	log.Infof("putting 20K independent key-value pairs (CM={}) to all nodes, minKeyIndex=%d, maxKeyIndex=%d, "+
			"valIndex=%d",
		sprayConf.minI, sprayConf.maxI, sprayConf.maxJ)
	if _, err = SprayPuts(sprayConf); err != nil {
		log.Errorf("failed to put independent key-value pairs: %v", err)
		return score
	}
	score += 10
	log.WithField("score", score).Info("score +10 - put 20K independent key-value pairs successful")

	// Sleep
	log.Info("sleeping for 11s")
	time.Sleep(11 * time.Second)

	// Key lists 1
	log.Info("getting key lists from all nodes")
	shardKeys1, err := TestKeyLists(view1Addrs, sprayConf.minI, sprayConf.maxI)
	if err != nil {
		log.Errorf("key list failed: %v", err)
		return score
	}
	nodeKeys1, err := NodeKeyLists(shardKeys1, view1)
	if err != nil {
		log.Errorf("failed to map nodes to keys: %v", err)
		return score
	}
	var counts []float64
	var sum float64 = 0.0
	for _, kl := range shardKeys1 {
		counts = append(counts, float64(len(kl)))
		sum += float64(len(kl))
	}
	avg := sum / float64(len(counts))

	for idx, count := range counts {
		diff := math.Abs(count - avg)
		if (diff / avg) >= 0.15 {
			log.Errorf("bad key distribution; shardCounts=%v, avg=%.2f, errorIndex=%d", counts, avg, idx)
			return score
		}
	}
	score += 10
	log.WithField("score", score).Info("score +10 - key distribution (with <=15% deviation) successful")

	// PUT view 2
	log.Infof("putting view 2 to the nodes (%s)", v2.String())
	view2Addrs := allAddrs[:v2.NumNodes]
	statusCode, err = kvs4client.PutView(view1Addrs[v2.NumNodes-1], kvs4client.ViewReq{Nodes: view2Addrs, NumShards: v2.NumShards})
	if err != nil {
		log.Errorf("failed to put view: %v", err)
		return score
	}
	if statusCode != 200 {
		log.WithFields(logrus.Fields{
			"expected": 200,
			"received": statusCode,
		}).Error("bad status code for put view")
		return score
	}
	log.Info("put view 2 successful")

	// GET view 2
	log.Info("getting views from nodes and checking consistency")
	var view2 kvs4client.ViewResp
	if view2, err = TestViewsConsistent(view1Addrs, v1); err != nil {
		log.Errorf("test failed: %v", err)
		return score
	}
	log.Info("get view from all nodes successful and all views consistent")

	// Key lists 2
	log.Info("getting key lists from all nodes")
	shardKeys2, err := TestKeyLists(view2Addrs, sprayConf.minI, sprayConf.maxI)
	if err != nil {
		log.Errorf("key list failed: %v", err)
		return score
	}
	nodeKeys2, err := NodeKeyLists(shardKeys2, view2)
	if err != nil {
		log.Errorf("failed to map nodes to keys: %v", err)
		return score
	}

	totalMovement := 0
	// TODO: iterate over view1 nodes and count diffs.
	for idx, kl := range shardKeys1 {
		notInKl2 := 0
		for _, k := range kl {
			if sK
		}
	}

	return score
}
