package kvs4

import (
	"time"

	"github.com/sirupsen/logrus"

	"github.com/AKarbas/cse138-kuber-grader/pkg/k8s"
)

const ViewChangeMaxScore = 40

func ViewChangeTest(c TestConfig, v1, v2 ViewConfig, killNodes bool) int {
	log := logrus.New().WithFields(logrus.Fields{
		"test":  "ViewChange",
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
			"6. if killNodes==true, kill all but one node from each shard; " +
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
		numNodes = max(v1.NumShards, v2.NumNodes)
	}
	if err := k8sClient.CreatePods(c.Namespace, c.GroupName, c.Image(), 1, numNodes); err != nil {
		log.Errorf("test start failed; failed to create pods: %v", err)
		return score
	}
	defer PostTestCleanup(k8sClient, c.Namespace, c.GroupName)

	log.Info("nodes created, sleeping for 10s (to let nodes start up)")
	time.Sleep(10 * time.Second)

	return score
}
