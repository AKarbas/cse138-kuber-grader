package kvs4

import (
	"time"

	"github.com/sirupsen/logrus"

	"github.com/AKarbas/cse138-kuber-grader/pkg/k8s"
	"github.com/AKarbas/cse138-kuber-grader/pkg/kvs4client"
)

const AvailabilityMaxScore = 50

func AvailabilityTest(c Config, v ViewConfig) int {
	log := logrus.New().WithFields(logrus.Fields{
		"test":  "Availability",
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
	defer func(s *int) {
		log.WithField("finalScore", s).Info("test completed.")
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
		log.Errorf("test failed: %v", err)
		return score
	}
	log.Info("get view from all nodes successful and all views consistent")

	// Partition
	var partitions [][]string
	log.Info("Partitioning the nodes")
	if partitions, err = partitionNodes(k8sClient, c, view, addrMappings); err != nil {
		log.Errorf("failed to isolate partitions: %v", err)
		return score
	}

	endpoints := make([]string, len(partitions))
	for idx, part := range partitions {
		endpoints[idx] = part[0]
	}

	return score
}

func partitionNodes(
	kc k8s.Client, c Config, v kvs4client.ViewResp, m map[string]k8s.AddrLabelMapping,
) ([][]string, error) {
	parts := genPartitions(v)
	for _, part := range parts {
		for _, addr := range part {
			if err := kc.IsolatePodByIps(c.Namespace, c.GroupName, m[addr].Index, part); err != nil {
				return nil, err
			}
		}
	}
	return parts, nil
}

func genPartitions(v kvs4client.ViewResp) [][]string {
	numParts := -1
	for _, s := range v.View {
		if numParts == -1 || numParts > len(s.Nodes) {
			numParts = len(s.Nodes)
		}
	}
	res := make([][]string, numParts)
	for _, s := range v.View {
		for idx, node := range s.Nodes {
			targetIdx := idx
			if targetIdx >= numParts {
				targetIdx = numParts - 1
			}
			res[targetIdx] = append(res[targetIdx], node)
		}
	}
	return res
}
