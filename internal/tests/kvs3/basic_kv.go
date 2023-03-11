package kvs3

import (
	"sort"
	"time"

	"github.com/sirupsen/logrus"
	"k8s.io/utils/strings/slices"

	"github.com/AKarbas/cse138-kuber-grader/pkg/k8s"
	"github.com/AKarbas/cse138-kuber-grader/pkg/kvs3client"
)

const (
	BasicKVMaxScore = 80
)

func BasicKVTest(conf TestConfig) int {
	log := logrus.New().WithFields(logrus.Fields{
		"test":     "BasicKeyVal",
		"group":    conf.GroupName,
		"numNodes": conf.NumNodes,
		"numKeys":  conf.NumKeys,
	})
	log.Infof("this test runs on a healthy network and checks "+
		"if simple view and data operations are successful. "+
		"max score in test: %d", BasicKVMaxScore)
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

	success := true
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
		}).Warn("bad status code for put view")
		success = false
	}
	if success {
		score += 10
		log.Info("score +10 - put view successful")
	}

	time.Sleep(10 * time.Second)

	success = true
	view, statusCode, err := kvs3client.GetView(addresses[conf.NumNodes-1])
	if err != nil {
		log.Errorf("failed to get view: %v", err)
		return score
	}
	if statusCode != 200 {
		log.WithFields(logrus.Fields{
			"expected": 200,
			"received": statusCode,
		}).Warn("bad status code for get view")
		success = false
	}
	sort.Strings(view)

	if !slices.Equal(addresses, view) {
		log.WithFields(logrus.Fields{
			"expected": addresses,
			"received": view,
		}).Warn("view not consistent")
		success = false
	}
	if success {
		score += 10
		log.Info("score +10 - view consistent")
	}

	var cm kvs3client.CausalMetadata = nil

	success = true
	for i := 0; i < conf.NumKeys; i++ {
		_, cm, statusCode, err = kvs3client.GetKey(
			addresses[i%conf.NumNodes],
			key(i),
			cm,
		)
		if err != nil {
			log.Errorf("failed to get key: %v", err)
			return score
		}
		if statusCode != 404 {
			log.WithFields(logrus.Fields{
				"expected": 404,
				"received": statusCode,
			}).Warn("invalid status code for get")
			success = false
		}
	}
	if success {
		score += 10
		log.Info("score +10 - first gets successful")
	}

	success = true
	for i := 0; i < conf.NumKeys; i++ {
		cm, statusCode, err = kvs3client.PutKeyVal(
			addresses[(i+1)%conf.NumNodes],
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
			}).Warn("invalid status code for put")
			success = false
		}
	}
	if success {
		score += 10
		log.Info("score +10 - first puts successful")
	}

	success = true
	for i := 0; i < conf.NumKeys; i++ {
		var value string
		value, cm, statusCode, err = kvs3client.GetKey(
			addresses[(i+2)%conf.NumNodes],
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
			}).Warn("invalid status code for get")
			success = false
		}
		expected := val(i, 0)
		if value != expected {
			log.WithFields(logrus.Fields{
				"expected": expected,
				"received": value,
			}).Warn("invalid value")
			success = false
		}
	}
	if success {
		score += 10
		log.Info("score +10 - second gets successful")
	}

	success = true
	for i := 0; i < conf.NumKeys; i++ {
		cm, statusCode, err = kvs3client.PutKeyVal(
			addresses[(i+3)%conf.NumNodes],
			key(i),
			val(i, 1),
			cm,
		)
		if err != nil {
			log.Errorf("failed to put key-val: %v", err)
			return score
		}
		if statusCode != 200 {
			log.WithFields(logrus.Fields{
				"expected": 200,
				"received": statusCode,
			}).Warn("invalid status code for put")
			success = false
		}
	}
	if success {
		score += 10
		log.Info("score +10 - second puts successful")
	}

	success = true
	for i := 0; i < conf.NumKeys; i++ {
		var value string
		value, cm, statusCode, err = kvs3client.GetKey(
			addresses[(i+4)%conf.NumNodes],
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
			}).Warn("invalid status code for get")
			success = false
		}
		expected := val(i, 1)
		if value != expected {
			log.WithFields(logrus.Fields{
				"expected": expected,
				"received": value,
			}).Warn("invalid value")
			success = false
		}
	}
	if success {
		score += 10
		log.Info("score +10 - third gets successful")
	}

	success = true
	var keyCount int
	var keys []string
	keyCount, keys, cm, statusCode, err = kvs3client.GetKeyList(addresses[0], cm)
	if err != nil {
		log.Errorf("failed to get key list: %v", err)
		return score
	}
	if statusCode != 200 {
		log.WithFields(logrus.Fields{
			"expected": 200,
			"received": statusCode,
		}).Warn("invalid status code for get key list")
		success = false
	}
	if keyCount != conf.NumKeys {
		log.WithFields(logrus.Fields{
			"expected": conf.NumKeys,
			"received": keyCount,
		}).Warn("invalid key count for get key list")
		success = false
	}
	expected := []string{}
	for i := 0; i < conf.NumKeys; i++ {
		expected = append(expected, key(i))
	}
	sort.Strings(keys)
	if !slices.Equal(expected, keys) {
		log.Warn("invalid key list for get key list")
		success = false
	}
	if success {
		score += 10
		log.Info("score +10 - key list valid")
	}

	return score
}
