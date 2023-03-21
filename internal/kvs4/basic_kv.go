package kvs4

import (
	"time"

	"github.com/sirupsen/logrus"

	"github.com/AKarbas/cse138-kuber-grader/pkg/k8s"
	"github.com/AKarbas/cse138-kuber-grader/pkg/kvs4client"
)

const BasicKVMaxScore = 60

func BasicKvTest(c TestConfig, v ViewConfig) int {
	log := logrus.New().WithFields(logrus.Fields{
		"test":  "BasicKeyVal",
		"group": c.GroupName,
	})
	log.WithField("viewConfig", v.String()).Info(
		"starting test. Steps: " +
			"1. create a cluster (launch processes; wait 10s; PUT view); " +
			"2. get the view from all nodes and expect consistency; " +
			"3. do non-causally-dependent writes (all with CM={}) sprayed across all nodes (keys [1, N]); " +
			"4. do causally-dependent writes (use CM received after first req in second and so on) sprayed across" +
			" all nodes (keys [N+1, 2N]); " +
			"5. do reads on writes of step 4 (from all nodes, all with reused CM (frm previous access or the end of step 4))" +
			" and expect latest values; " +
			"6. wait for eventual consistency (10s); " +
			"7. do reads on writes of step 3 (from all nodes, all with CM={}) and expect consistent values from" +
			" all nodes (tie-breaking). " +
			"Steps 1-5 and 7 each have 10 points for a total of 60.",
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

	if err := k8sClient.CreatePods(c.Namespace, c.GroupName, c.Image(), 1, v.NumNodes); err != nil {
		log.Errorf("test start failed; failed to create pods: %v", err)
		return score
	}
	defer PostTestCleanup(k8sClient, c.Namespace, c.GroupName)

	log.Info("nodes created, sleeping for 10s (to let nodes start up)")
	time.Sleep(10 * time.Second)

	// PUT view
	addresses, err := k8sClient.ListPodAddresses(c.Namespace, k8s.GroupLabels(c.GroupName))
	if err != nil {
		log.Errorf("test start failed; failed to list pod addresses: %v", err)
		return score
	}

	log.Info("putting view to the nodes")
	statusCode, err := kvs4client.PutView(addresses[len(addresses)-1], kvs4client.ViewReq{Nodes: addresses, NumShards: v.NumShards})
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
	score += 10
	log.WithField("score", score).Info("score +10 - put view successful")

	log.Info("sleeping for 10s (to let nodes set up the view)")
	time.Sleep(10 * time.Second)

	// GET view
	log.Info("getting views from nodes and checking consistency")
	if _, err := TestViewsConsistent(addresses, v); err != nil {
		log.Errorf("test failed: %v", err)
		return score
	}
	score += 10
	log.WithField("score", score).Info("score +10 - get view from all nodes successful and all views consistent")

	// Independent Puts
	independentSprayConf := SprayConfig{
		addresses:           addresses,
		minI:                1,
		maxI:                v.NumNodes,
		minJ:                1,
		maxJ:                3,
		cm:                  nil,
		noCm:                true,
		acceptedStatusCodes: []int{200, 201},
	}
	log.Infof("putting independent key-value pairs (CM={}) to all nodes, minKeyIndex=%d, maxKeyIndex=%d, "+
		"minValIndexPerKey=%d, maxValIndexPerKey=%d",
		independentSprayConf.minI, independentSprayConf.maxI, independentSprayConf.minJ, independentSprayConf.maxJ)
	if _, err = SprayPuts(independentSprayConf); err != nil {
		log.Errorf("failed to put independent key-value pairs: %v", err)
		return score
	}
	score += 10
	log.WithField("score", score).Info("score +10 - put independent key-value pairs successful")

	// Dependent Puts
	dependentSprayConf := SprayConfig{
		addresses:           addresses,
		minI:                v.NumNodes + 1,
		maxI:                2 * v.NumNodes,
		minJ:                1,
		maxJ:                3,
		cm:                  nil,
		noCm:                false,
		acceptedStatusCodes: []int{200, 201},
	}
	log.Infof("putting dependent key-value pairs (reusing CM) to all nodes, minKeyIndex=%d, maxKeyIndex=%d, "+
		"minValIndexPerKey=%d, maxValIndexPerKey=%d",
		dependentSprayConf.minI, dependentSprayConf.maxI, dependentSprayConf.minJ, dependentSprayConf.maxJ)

	if dependentSprayConf.cm, err = SprayPuts(dependentSprayConf); err != nil {
		log.Errorf("failed to put dependent key-value pairs: %v", err)
		return score
	}
	score += 10
	log.WithField("score", score).Info("score +10 - put dependent key-value pairs successful")

	// Dependent Gets
	dependentSprayConf.minJ = dependentSprayConf.maxJ
	dependentSprayConf.acceptedStatusCodes = []int{200}
	log.Infof("getting dependent key-value pairs (reusing CM) from all nodes and expecting latest value, "+
		"minKeyIndex=%d, maxKeyIndex=%d, expectedValIndex=%d",
		dependentSprayConf.minI, dependentSprayConf.maxI, dependentSprayConf.maxJ)
	if dependentSprayConf.cm, err = SprayGets(dependentSprayConf); err != nil {
		log.Errorf("failed to get dependent key-value pairs: %v", err)
		return score
	}
	score += 10
	log.WithField("score", score).Info("score +10 - get dependent key-value pairs successful")

	// Sleep
	log.Info("sleeping for 11s (to let nodes become eventually consistent)")
	time.Sleep(11 * time.Second)

	// Independent Gets
	independentSprayConf.acceptedStatusCodes = []int{200}
	log.Infof("getting independent key-value pairs (with CM={}) from all nodes and expecting consistent values, "+
		"minKeyIndex=%d, maxKeyIndex=%d, minValIndexPerKey=%d, maxValIndexPerKey=%d",
		independentSprayConf.minI, independentSprayConf.maxI, independentSprayConf.minJ, independentSprayConf.maxJ)
	if _, err := SprayGets(independentSprayConf); err != nil {
		log.Errorf("failed to get independent key-value pairs: %v", err)
		return score
	}
	score += 10
	log.WithField("score", score).Info("score +10 - get independent key-value pairs successful")

	return score
}
