package kvs3

import (
	"fmt"
	"sort"
	"time"

	"github.com/sirupsen/logrus"
	"k8s.io/utils/strings/slices"

	"github.com/AKarbas/cse138-kuber-grader/pkg/k8s"
	"github.com/AKarbas/cse138-kuber-grader/pkg/kvs3client"
)

const (
	kBasicKVMaxScore = 80
)

func key(i int) string {
	return fmt.Sprintf("Key-%d", i)
}

func val(i, j int) string {
	return fmt.Sprintf("Val-%d-%d", i, j)
}

func BasicKVTest(conf TestConfig) int {
	log := logrus.New().WithFields(logrus.Fields{
		"test":     "BasicKeyVal",
		"group":    conf.GroupName,
		"numNodes": conf.NumNodes,
		"numKeys":  conf.NumKeys,
	})
	log.Infof("this test runs on a healthy network and checks "+
		"if simple view and data operations are successful"+
		"max score in test: %d", kBasicKVMaxScore)
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
	defer k8sClient.DeletePods(conf.Namespace, k8s.GroupLabels(conf.GroupName)) // cleanup

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
	score += 10
	log.Info("score +10 - put view successful")

	view, statusCode, err := kvs3client.GetView(addresses[conf.NumNodes-1])
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
	sort.Strings(view)

	if !slices.Equal(addresses, view) {
		log.WithFields(logrus.Fields{
			"expected": addresses,
			"received": view,
		}).Error("view not consistent")
		return score
	}
	score += 10
	log.Info("score +10 - view consistent")

	var cm kvs3client.CausalMetadata = nil

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
			}).Error("invalid status code for get")
			return score
		}
	}
	score += 10
	log.Info("score +10 - first gets successful")

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
			}).Error("invalid status code for put")
			return score
		}
	}
	score += 10
	log.Info("score +10 - first puts successful")

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
	log.Info("score +10 - second gets successful")

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
			}).Error("invalid status code for put")
			return score
		}
	}
	score += 10
	log.Info("score +10 - second puts successful")

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
			}).Error("invalid status code for get")
			return score
		}
		expected := val(i, 1)
		if value != expected {
			log.WithFields(logrus.Fields{
				"expected": expected,
				"received": value,
			}).Error("invalid value")
			return score
		}
	}
	score += 10
	log.Info("score +10 - third gets successful")

	keyCount, keys, cm, statusCode, err := kvs3client.GetKeyList(addresses[0], cm)
	if err != nil {
		log.Errorf("failed to get key list: %v", err)
		return score
	}
	if statusCode != 200 {
		log.WithFields(logrus.Fields{
			"expected": 200,
			"received": statusCode,
		}).Error("invalid status code for get key list")
		return score
	}
	if keyCount != conf.NumKeys {
		log.WithFields(logrus.Fields{
			"expected": conf.NumKeys,
			"received": keyCount,
		}).Error("invalid key count for get key list")
		return score
	}
	expected := []string{}
	for i := 0; i < conf.NumKeys; i++ {
		expected = append(expected, key(i))
	}
	sort.Strings(keys)
	if !slices.Equal(expected, keys) {
		log.Error("invalid key list for get key list")
		return score
	}
	score += 10
	log.Info("score +10 - key list valid")

	return score
}
