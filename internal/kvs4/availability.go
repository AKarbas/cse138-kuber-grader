package kvs4

import (
	"time"

	"github.com/sirupsen/logrus"

	"github.com/AKarbas/cse138-kuber-grader/pkg/k8s"
	"github.com/AKarbas/cse138-kuber-grader/pkg/kvs4client"
)

const AvailabilityMaxScore = 50

func AvailabilityTest(c TestConfig, v ViewConfig) int {
	log := logrus.New().WithFields(logrus.Fields{
		"test":  "availability",
		"group": c.GroupName,
	})
	log.WithField("viewConfig", v.String()).Info(
		"starting test. Steps: " +
			"1. create a cluster (launch processes; wait 10s; PUT view); " +
			"2. get the view from all nodes and expect consistency; " +
			"3. partition the nodes so that each partition contains 1+ node from each shard; " +
			"4. do non-causally-dependent writes (all with CM={}) sprayed across all partitions (keys [1, N]); " +
			"5. do causally-dependent writes (use CM received after first req in second and so on) sprayed across" +
			" all partitions (keys [N+1, 2N]); " +
			"6. do reads on writes of step 5 (from all partitions, all with reused CM (frm previous access or the" +
			" end of step 5)) and expect latest values or stall-fails; " +
			"7. heal the network and wait for eventual consistency (10s); " +
			"8. do reads on writes of step 4 and 5 (from all nodes, all with CM={}) and expect consistent values from" +
			" all nodes (tie-breaking) for keys [1, N] and latest values for keys [N+1, 2N]. " +
			"Steps 4-6 each have 10 points and step 8 has 20 points for a total of 50.",
	)

	k8sClient := k8s.Client{}
	score := 0
	defer func() {
		log.Info("Here are your process logs (for finding what went wrong...)")
		logs, err := k8sClient.GetPodLogs(c.Namespace, k8s.GroupLabels(c.GroupName))
		if err != nil {
			log.Errorf("failed to get pods' logs: %v", err)
			return
		}
		for idx, l := range logs {
			log.Infof("log idx=%d (indices not stable): %s", idx, l)
		}
	}()
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
	addrMappings, err := k8sClient.ListAddressGroupIndexMappings(c.Namespace, k8s.GroupLabels(c.GroupName))
	if err != nil {
		log.Errorf("test start failed; failed to list pod addresses: %v", err)
		return score
	}
	log.Info("putting view to the nodes")
	addresses := k8s.PodAddrsFromMappings(addrMappings)
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
	log.Info("put view successful")

	log.Info("sleeping for 10s (to let nodes set up the view)")
	time.Sleep(10 * time.Second)

	// GET view
	log.Info("getting views from nodes and checking consistency")
	var view kvs4client.ViewResp
	if view, err = TestViewsConsistent(addresses, v); err != nil {
		log.Errorf("get view failed: %v", err)
		return score
	}
	log.Info("get view from all nodes successful and all views consistent")

	// Partition
	var partitions [][]string
	log.Info("Partitioning the nodes")
	if partitions, err = partitionNodes(k8sClient, c, view, addrMappings); err != nil {
		log.Errorf("failed to isolate pod partitions: %v", err)
		return score
	}

	partitionEndpoints := make([]string, len(partitions))
	for idx, part := range partitions {
		partitionEndpoints[idx] = part[0]
	}

	// Independent Puts
	independentSprayConf := SprayConfig{
		addresses:           partitionEndpoints,
		minI:                1,
		maxI:                v.NumNodes,
		minJ:                1,
		maxJ:                3,
		cm:                  nil,
		noCm:                true,
		acceptedStatusCodes: []int{200, 201},
	}
	log.Infof("putting independent key-value pairs (CM={}) to all partitions, minKeyIndex=%d, maxKeyIndex=%d, "+
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
		addresses:           partitionEndpoints,
		minI:                v.NumNodes + 1,
		maxI:                2 * v.NumNodes,
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
		log.Errorf("failed to put dependent key-value pairs: %v", err)
		return score
	}
	score += 10
	log.WithField("score", score).Info("score +10 - put dependent key-value pairs successful")

	// Dependent Gets
	dependentSprayConf.minJ = dependentSprayConf.maxJ
	dependentSprayConf.acceptedStatusCodes = []int{200, 500, 503}
	log.Infof("getting dependent key-value pairs (reusing CM) from all partitions and expecting latest value or "+
		"stall-fail, minKeyIndex=%d, maxKeyIndex=%d, expectedValIndex=%d",
		dependentSprayConf.minI, dependentSprayConf.maxI, dependentSprayConf.maxJ)
	if dependentSprayConf.cm, err = SprayGets(dependentSprayConf); err != nil {
		log.Warnf("failed to get dependent key-value pairs: %v", err)
	} else {
		score += 10
		log.WithField("score", score).Info("score +10 - get dependent key-value pairs successful")
	}

	// Heal network
	log.Info("healing network partitions")
	if err = k8sClient.DeleteNetPolicies(c.Namespace, k8s.GroupLabels(c.GroupName)); err != nil {
		log.Errorf("failed to delete pod network policies: %v", err)
		return score
	}
	// Sleep
	log.Info("sleeping for 11s (to let nodes become eventually consistent)")
	time.Sleep(11 * time.Second)

	// Dependent Gets
	dependentSprayConf.addresses = addresses
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
	independentSprayConf.addresses = addresses
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
}

func partitionNodes(
	kc k8s.Client, c TestConfig, v kvs4client.ViewResp, m map[string]k8s.PodMetaDetails,
) ([][]string, error) {
	parts := GenPartitions(v)
	for _, part := range parts {
		var partIps []string
		for _, addr := range part {
			partIps = append(partIps, m[addr].Ip)
		}
		for _, addr := range part {
			if err := kc.IsolatePodByIps(c.Namespace, c.GroupName, m[addr].Index, partIps); err != nil {
				return nil, err
			}
		}
	}
	return parts, nil
}
