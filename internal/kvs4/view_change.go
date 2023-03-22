package kvs4

import (
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/AKarbas/cse138-kuber-grader/pkg/k8s"
	"github.com/AKarbas/cse138-kuber-grader/pkg/kvs4client"
)

const ViewChangeMaxScore = 40

func ViewChangeTest(c TestConfig, v1, v2 ViewConfig, killNodes bool) int {
	log := logrus.New().WithFields(logrus.Fields{
		"test":  "viewChange",
		"group": c.GroupName,
	})
	log.WithFields(logrus.Fields{
		"viewConfig1": v1.String(),
		"viewConfig2": v2.String(),
		"killNodes":   killNodes,
	}).Info(
		"starting test. Steps: " +
			"1. create all needed nodes for the test some of which may be killed at step 6 (launch processes; wait 10s); " +
			"2. put viewConfig1; " +
			"3. get the view from all nodes and expect consistency; " +
			"4. do non-causally-dependent writes (all with CM={}) sprayed across all nodes (keys [1, N]); " +
			"5. do causally-dependent writes (use CM received after first req in second and so on) sprayed across" +
			" all nodes (keys [N+1, 2N]); " +
			"6. if killNodes==true, wait for eventual consistency and then kill all but one node from each shard; " +
			"7. put viewConfig2 (possibly with new nodes if viewConfig2 has more nodes or some have been killed); " +
			"8. get the view from all nodes and expect consistency; " +
			"9. do reads on writes of step 4 and 5 (from all current nodes, all with CM={}) and expect consistent values" +
			" from all nodes (tie-breaking) for keys [1, N] and latest values for keys [N+1, 2N]. " +
			"Steps 7-8 each have 10 points and step 9 has 20 points for a total of 40.",
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

	numNodes := max(v1.NumNodes, v2.NumNodes)
	if killNodes {
		numNodes = v1.NumNodes + max(v2.NumNodes-v1.NumShards, 0)
	}
	if err := k8sClient.CreatePods(c.Namespace, c.GroupName, c.Image(), 1, numNodes); err != nil {
		log.Errorf("test start failed; failed to create pods: %v", err)
		return score
	}
	defer PostTestCleanup(k8sClient, c.Namespace, c.GroupName)

	log.Info("nodes created, sleeping for 10s (to let nodes start up)")
	time.Sleep(10 * time.Second)

	// PUT view 1
	allAddrMappings, err := k8sClient.ListAddressGroupIndexMappings(c.Namespace, k8s.GroupLabels(c.GroupName))
	if err != nil {
		log.Errorf("test start failed; failed to list pod addresses: %v", err)
		return score
	}
	log.Infof("putting view 1 to the nodes (%s)", v1.String())
	allAddrs := k8s.PodAddrsFromMappings(allAddrMappings)
	view1Addrs := allAddrs[:v1.NumNodes]
	if len(view1Addrs) != v1.NumNodes {
		panic(fmt.Errorf("too few nodeAddrs for view 1, addrs=%v, n1=%d", view1Addrs, v1.NumNodes))
	}
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

	log.Info("sleeping for 10s (to let nodes set up the view)")
	time.Sleep(10 * time.Second)

	// GET view1
	log.Info("getting views from nodes and checking consistency")
	var view kvs4client.ViewResp
	if view, err = TestViewsConsistent(view1Addrs, v1); err != nil {
		log.Errorf("test failed: %v", err)
		return score
	}
	log.Info("get view from all nodes successful and all views consistent")

	// Independent Puts
	independentSprayConf := SprayConfig{
		addresses:           view1Addrs,
		minI:                1,
		maxI:                v1.NumNodes,
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
		log.Warnf("failed to put independent key-value pairs: %v", err)
	} else {
		log.Info("put independent key-value pairs successful")
	}

	// Dependent Puts
	dependentSprayConf := SprayConfig{
		addresses:           view1Addrs,
		minI:                v1.NumNodes + 1,
		maxI:                2 * v1.NumNodes,
		minJ:                1,
		maxJ:                3,
		cm:                  nil,
		noCm:                false,
		acceptedStatusCodes: []int{200, 201},
	}
	log.Infof("putting dependent key-value pairs (reusing CM) to all partitions, minKeyIndex=%d, maxKeyIndex=%d, "+
		"minValIndexPerKey=%d, maxValIndexPerKey=%d",
		dependentSprayConf.minI, dependentSprayConf.maxI, dependentSprayConf.minJ, dependentSprayConf.maxJ)

	if dependentSprayConf.cm, err = SprayPuts(dependentSprayConf); err != nil {
		log.Warnf("failed to put dependent key-value pairs: %v", err)
	} else {
		log.Info("put dependent key-value pairs successful")
	}

	log.Info("sleeping for 11s")
	time.Sleep(11 * time.Second)

	// Delete extra nodes
	var toKeep []string
	var toKill []string
	for _, s := range view.View {
		toKeep = append(toKeep, s.Nodes[0])
		toKill = append(toKill, s.Nodes[1:]...)
	}
	if killNodes {
		log.Info("killing all but one node from each shard")
		for _, addr := range toKill {
			if err = k8sClient.DeletePods(c.Namespace, k8s.PodLabelsNoBatch(c.GroupName, allAddrMappings[addr].Index)); err != nil {
				log.Errorf("failed to kill extra node: %v", err)
				return score
			}
		}
	}

	// PUT view 2
	log.Infof("putting view 2 to the nodes (%s)", v2.String())
	view2Addrs := allAddrs[:v2.NumNodes]
	if killNodes {
		view2Addrs = append(toKeep, allAddrs[v1.NumNodes:]...)
	}
	if len(view1Addrs) != v1.NumNodes {
		panic(fmt.Errorf("too few nodeAddrs for view 2, addrs=%v, n2=%d", view2Addrs, v2.NumNodes))
	}
	statusCode, err = kvs4client.PutView(view2Addrs[0], kvs4client.ViewReq{Nodes: view2Addrs, NumShards: v2.NumShards})
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
	log.WithField("score", score).Info("score +10 - put view 2 successful")

	log.Info("sleeping for 10s (to let nodes set up the view)")
	time.Sleep(10 * time.Second)

	// GET view2
	log.Info("getting views from nodes and checking consistency")
	if view, err = TestViewsConsistent(view2Addrs, v2); err != nil {
		log.Errorf("test failed: %v", err)
		return score
	}
	score += 10
	log.WithField("score", score).Info("score +10 - get view from all nodes successful and all views consistent")

	// Dependent Gets
	dependentSprayConf.addresses = view2Addrs
	dependentSprayConf.minJ = dependentSprayConf.maxJ
	dependentSprayConf.acceptedStatusCodes = []int{200}
	dependentSprayConf.cm = nil
	dependentSprayConf.noCm = true
	log.Infof("getting dependent key-value pairs (with CM={}) from all nodes and expecting latest value, "+
		"minKeyIndex=%d, maxKeyIndex=%d, expectedValIndex=%d",
		dependentSprayConf.minI, dependentSprayConf.maxI, dependentSprayConf.maxJ)
	if _, err = SprayGets(dependentSprayConf); err != nil {
		log.Warnf("failed to get dependent key-value pairs: %v", err)
	} else {
		score += 10
		log.WithField("score", score).Info("score +10 - get dependent key-value pairs successful")
	}
	// Independent Gets
	independentSprayConf.addresses = view2Addrs
	independentSprayConf.acceptedStatusCodes = []int{200}
	log.Infof("getting independent key-value pairs (with CM={}) from all nodes and expecting consistent values, "+
		"minKeyIndex=%d, maxKeyIndex=%d, minValIndexPerKey=%d, maxValIndexPerKey=%d",
		independentSprayConf.minI, independentSprayConf.maxI, independentSprayConf.minJ, independentSprayConf.maxJ)
	if _, err = SprayGets(independentSprayConf); err != nil {
		log.Warnf("failed to get independent key-value pairs: %v", err)
	} else {
		score += 10
		log.WithField("score", score).Info("score +10 - get independent key-value pairs successful")
	}

	return score

	return score
}
