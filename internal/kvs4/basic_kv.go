package kvs4

import (
	"reflect"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/AKarbas/cse138-kuber-grader/pkg/k8s"
	"github.com/AKarbas/cse138-kuber-grader/pkg/kvs4client"
)

const BasicKVMaxScore = 60

func BasicKvTest(c Config, v ViewConfig, n int) int {
	log := logrus.New().WithFields(logrus.Fields{
		"test":  "BasicKeyVal",
		"group": c.GroupName,
	})
	log.WithField("viewConfig", v.String()).Info(
		"starting test. Steps: " +
			"1. create a cluster (launch processes; wait 10s; PUT view); " +
			"2. get the view from all nodes and expect consistency; " +
			"3. do non-causally-dependent writes (all with CM={}) sprayed across all nodes (keys [1, N]); " +
			"4. do causally-dependent writes (use CM received first req in second and so on) sprayed across" +
			" all nodes (keys [N+1, 2N]); " +
			"5. do reads on writes of step 3 (from all nodes, all with CM received from last write of 3) and" +
			" expect latest values" +
			"6. wait for eventual consistency; " +
			"7. do reads on writes of step 2 (from all nodes, all with CM={}) and expect consistent values from" +
			" all nodes (tie-breaking). " +
			"Steps 1-5 and 7 each have 10 points for a total of 60.",
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

	if err := k8sClient.CreatePods(c.Namespace, c.GroupName, c.Image(), 1, n); err != nil {
		log.Errorf("test start failed; failed to create pods: %v", err)
		return score
	}
	defer PostTestCleanup(k8sClient, c.Namespace, c.GroupName)

	time.Sleep(10 * time.Second)

	// PUT view
	addresses, err := k8sClient.ListPodAddresses(c.Namespace, k8s.GroupLabels(c.GroupName))
	if err != nil {
		log.Errorf("test start failed; failed to list pod addresses: %v", err)
		return score
	}

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

	time.Sleep(10 * time.Second)

	// GET view
	receivedView := kvs4client.ViewResp{}
	for idx, addr := range addresses {
		resp, statusCode, err := kvs4client.GetView(addr)
		if err != nil {
			log.Errorf("failed to get view: %v", err)
			return score
		}
		if statusCode != 200 {
			log.WithFields(logrus.Fields{
				"expected": 200,
				"received": statusCode,
			}).Error("bad status code for get view")
			return score
		}

		if idx == 0 {
			nodeCounter := 0
			for _, s := range resp.View {
				nodeCounter += len(s.Nodes)
			}
			if nodeCounter != n {
				log.WithFields(logrus.Fields{
					"expected": n,
					"received": nodeCounter,
				}).Errorf("total number of nodes does not match; received view: %+v", resp)
			}
			receivedView = resp
			continue
		}

		if !reflect.DeepEqual(resp, receivedView) {
			log.WithFields(logrus.Fields{
				"expected": receivedView,
				"received": resp,
			}).Error("received views from different nodes do not match")
			return score
		}
	}
	score += 10
	log.WithField("score", score).Info("score +10 - get view from all nodes successful")

	return score
}
