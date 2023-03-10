package kvs3

import (
	"sort"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/AKarbas/cse138-kuber-grader/pkg/k8s"
	"github.com/AKarbas/cse138-kuber-grader/pkg/kvs3client"
)

const (
	PartitionedViewChangeMaxScore = 10
)

func PartitionedViewChangeTest(conf TestConfig) int {
	log := logrus.New().WithFields(logrus.Fields{
		"test":     "PartitionedViewChange",
		"group":    conf.GroupName,
		"numNodes": 3 * conf.NumNodes,
		"numKeys":  conf.NumKeys,
	})
	log.Infof("this test changes the view in a partitioned network; "+
		"heals the network and waits; and checks that the data are readable in "+
		"the new nodes after the view change. max score in test: %d",
		PartitionedViewChangeMaxScore)
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
		3,
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

	var err error
	batches := make([][]string, 3)
	for b := 0; b < 3; b++ {
		batches[b], err = k8sClient.ListPodAddresses(conf.Namespace, k8s.BatchLabels(conf.GroupName, b+1))
		if err != nil {
			log.Errorf("failed when listing node addresses: %v", err)
			return score
		}
		sort.Strings(batches[b])
	}
	firstTwo := append(batches[0], batches[1]...)
	firstAndThird := append(batches[0], batches[2]...)

	statusCode, err := kvs3client.PutView(batches[0][0], firstTwo)
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

	for b := 0; b < 3; b++ {
		err = k8sClient.IsolateBatch(conf.Namespace, conf.GroupName, b+1)
		if err != nil {
			log.Errorf("failed to isolate batch idx=%d: %v", b+1, err)
			return score
		}
	}
	defer k8sClient.DeleteNetPolicies(conf.Namespace, k8s.GroupLabels(conf.GroupName))

	var cm kvs3client.CausalMetadata = nil

	for i := 0; i < conf.NumKeys; i++ {
		cm, statusCode, err = kvs3client.PutKeyVal(
			firstTwo[i%len(firstTwo)],
			key(i),
			val(i, 0),
			cm,
		)
		if err != nil {
			log.Errorf("failed to put key-val: %v", err)
			return score
		}
		if statusCode != 201 {
			log.WithFields(logrus.Fields{
				"expected": 201,
				"received": statusCode,
			}).Error("invalid status code for put")
			return score
		}
	}

	k8sClient.DeleteNetPolicies(conf.Namespace, k8s.GroupLabels(conf.GroupName))

	time.Sleep(11 * time.Second)

	statusCode, err = kvs3client.PutView(batches[0][0], firstAndThird)
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

	for i := 0; i < conf.NumKeys; i++ {
		var value string
		value, cm, statusCode, err = kvs3client.GetKey(
			batches[2][i%conf.NumNodes],
			key(i),
			cm,
		)
		if err != nil {
			log.Errorf("failed to get key: %v", err)
			return score
		}
		if statusCode != 200 {
			log.WithFields(logrus.Fields{
				"expected": 200,
				"received": statusCode,
			}).Error("invalid status code for get")
			return score
		}
		expected := val(i, 0)
		if value != expected {
			log.WithFields(logrus.Fields{
				"expected": expected,
				"received": value,
			}).Error("invalid value")
			return score
		}
	}
	score += 10
	log.Info("score +10 - gets from new nodes after partition heal successful")

	return score
}
