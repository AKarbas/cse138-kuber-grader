package kvs3

import (
	"sort"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/AKarbas/cse138-kuber-grader/pkg/k8s"
	"github.com/AKarbas/cse138-kuber-grader/pkg/kvs3client"
)

const (
	AvailabilityMaxScore = 10
)

func AvailabilityTest(conf TestConfig) int {
	log := logrus.New().WithFields(logrus.Fields{
		"test":     "Availability",
		"group":    conf.GroupName,
		"numNodes": conf.NumNodes,
		"numKeys":  conf.NumKeys,
	})
	log.Infof("this test isolates each node and ensures that it's writable; and that "+
		"after partitions are healed, all nodes contain all of the data. max score in test: %d",
		AvailabilityMaxScore)
	k8sClient := k8s.Client{}

	score := 0
	defer func(s *int) {
		log.Infof("final score: %d", *s)
	}(&score)

	if err := k8sClient.DeletePods(conf.Namespace, k8s.GroupLabels(conf.GroupName)); err != nil {
		log.Errorf("failed to delete pods: %v", err)
		return score
	}
	if err := k8sClient.AwaitDeletion(conf.Namespace, k8s.GroupLabels(conf.GroupName)); err != nil {
		log.Errorf("failed when awaiting deletion of pods: %v", err)
		return score
	}
	if err := k8sClient.DeleteNetPolicies(conf.Namespace, k8s.GroupLabels(conf.GroupName)); err != nil {
		log.Errorf("failed to delete network policies: %v", err)
		return score
	}

	if err := k8sClient.CreatePods(
		conf.Namespace,
		conf.GroupName,
		conf.Image(),
		1,
		conf.NumNodes,
	); err != nil {
		log.Errorf("could not create nodes: %v", err)
		return score
	}
	defer func() {
		k8sClient.DeletePods(conf.Namespace, k8s.GroupLabels(conf.GroupName))
		k8sClient.AwaitDeletion(conf.Namespace, k8s.GroupLabels(conf.GroupName))
	}() // cleanup

	time.Sleep(10 * time.Second)

	addresses, err := k8sClient.ListPodAddresses(conf.Namespace, k8s.GroupLabels(conf.GroupName))
	if err != nil {
		log.Errorf("failed when listing node addresses: %v", err)
		return score
	}
	sort.Strings(addresses)

	statusCode, err := kvs3client.PutView(addresses[0], addresses)
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

	time.Sleep(11 * time.Second)

	for i := 0; i < conf.NumNodes; i++ {
		err = k8sClient.IsolatePod(conf.Namespace, conf.GroupName, i+1)
		if err != nil {
			log.Errorf("failed to isolate pod idx=%d: %v", i+1, err)
			return score
		}
	}
	defer k8sClient.DeleteNetPolicies(conf.Namespace, k8s.GroupLabels(conf.GroupName))

	partitionCms := make([]kvs3client.CausalMetadata, conf.NumNodes)

	success := true
	for k := 0; k < conf.NumKeys; k++ {
		for i := 0; i < conf.NumNodes; i++ {
			partitionId := (i + k) % conf.NumNodes
			partitionCms[partitionId], statusCode, err = kvs3client.PutKeyVal(
				addresses[partitionId],
				key(k),
				val(k, i),
				partitionCms[partitionId],
			)
			if err != nil {
				log.Errorf("failed to put key-val: %v", err)
				return score
			}
			if statusCode != 201 && statusCode != 200 {
				log.WithFields(logrus.Fields{
					"expected": "200|201",
					"received": statusCode,
				}).Warn("invalid status code for put")
				success = false
			}
		}
	}

	k8sClient.DeleteNetPolicies(conf.Namespace, k8s.GroupLabels(conf.GroupName))

	time.Sleep(11 * time.Second)

	for k := 0; k < conf.NumKeys; k++ {
		for i := 0; i < conf.NumNodes; i++ {
			var value string
			value, _, statusCode, err = kvs3client.GetKey(
				addresses[(i+k+1)%conf.NumNodes],
				key(k),
				nil,
			)
			if err != nil {
				log.Errorf("failed to get key: %v", err)
				return score
			}
			if statusCode != 200 {
				log.WithFields(logrus.Fields{
					"expected": 200,
					"received": statusCode,
				}).Warn("invalid status code for get")
				success = false
			}
			if len(value) < len(val(k, 0)) {
				log.WithFields(logrus.Fields{
					"minExpected": len(val(k, 0)),
					"received":    len(value),
				}).Warn("value too small")
				success = false
			}
		}
	}
	if success {
		score += 10
		log.Info("score +10 - gets from new nodes after partition heal successful")
	}

	return score
}
