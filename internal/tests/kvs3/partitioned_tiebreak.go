package kvs3

import (
	"sort"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/AKarbas/cse138-kuber-grader/pkg/k8s"
	"github.com/AKarbas/cse138-kuber-grader/pkg/kvs3client"
)

const (
	PartitionedTotalOrderMaxScore = 50
)

func PartitionedTotalOrderTest(conf TestConfig) int {
	log := logrus.New().WithFields(logrus.Fields{
		"test":     "PartitionedTieBreak",
		"group":    conf.GroupName,
		"numNodes": 2 * conf.NumNodes,
		"numKeys":  conf.NumKeys,
	})
	log.Infof("this test starts a cluster and then partitions the nodes "+
		"into two parts; inserts to both parts; heals the partition; and expects "+
		"the results to be the same from all nodes, including causal ordering and "+
		"tie breaking. max score in test: %d", PartitionedTotalOrderMaxScore)
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
		2,
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

	batches := make([][]string, 2)
	for b := 0; b < 2; b++ {
		batches[b], err = k8sClient.ListPodAddresses(conf.Namespace, k8s.BatchLabels(conf.GroupName, b+1))
		if err != nil {
			log.Errorf("failed when listing node addresses: %v", err)
			return score
		}
		sort.Strings(batches[b])
	}

	time.Sleep(11 * time.Second)

	for b := 0; b < 2; b++ {
		err = k8sClient.IsolateBatch(conf.Namespace, conf.GroupName, b+1)
		if err != nil {
			log.Errorf("failed to isolate batch idx=%d: %v", b+1, err)
			return score
		}
	}
	defer k8sClient.DeleteNetPolicies(conf.Namespace, k8s.GroupLabels(conf.GroupName))

	var cm kvs3client.CausalMetadata = nil

	// First batch of causal puts
	for i := 0; i < conf.NumKeys; i++ {
		cm, statusCode, err = kvs3client.PutKeyVal(
			batches[i%2][i%conf.NumNodes],
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

	// first batch of non-causal puts (CM = {})
	for i := conf.NumKeys; i < 2*conf.NumKeys; i++ {
		_, statusCode, err = kvs3client.PutKeyVal(
			batches[i%2][i%conf.NumNodes],
			key(i),
			val(i, 0),
			nil,
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

	// Second batch of causal puts
	for i := 0; i < conf.NumKeys; i++ {
		cm, statusCode, err = kvs3client.PutKeyVal(
			batches[(i+1)%2][i%conf.NumNodes],
			key(i),
			val(i, 1),
			cm,
		)
		if err != nil {
			log.Errorf("failed to put key-val: %v", err)
			return score
		}
		if statusCode != 200 && statusCode != 201 {
			log.WithFields(logrus.Fields{
				"expected": "200|201",
				"received": statusCode,
			}).Error("invalid status code for put")
			return score
		}
	}

	// Second batch of non-causal puts (CM = {})
	for i := conf.NumKeys; i < 2*conf.NumKeys; i++ {
		_, statusCode, err = kvs3client.PutKeyVal(
			batches[(i+1)%2][i%conf.NumNodes],
			key(i),
			val(i, 1),
			nil,
		)
		if err != nil {
			log.Errorf("failed to put key-val: %v", err)
			return score
		}
		if statusCode != 200 && statusCode != 201 {
			log.WithFields(logrus.Fields{
				"expected": "200|201",
				"received": statusCode,
			}).Error("invalid status code for put")
			return score
		}
	}

	score += 10
	log.Info("score +10 - partitioned puts successful")

	// Test correct/stall reads
	for b := 0; b < 2; b++ {
		i := conf.NumKeys - 1
		var value string
		value, cm, statusCode, err = kvs3client.GetKey(
			batches[b][i%conf.NumNodes],
			key(i),
			cm,
		)
		if err != nil {
			log.Errorf("failed to get key: %v", err)
			return score
		}
		if statusCode != 200 && statusCode != 500 {
			log.WithFields(logrus.Fields{
				"expected": "200|500",
				"received": statusCode,
			}).Error("invalid status code for get")
			return score
		}
		if statusCode == 200 && value != val(i, 1) {
			log.WithFields(logrus.Fields{
				"expected": val(i, 1),
				"received": value,
			}).Error("invalid value")
			return score
		}
	}

	score += 10
	log.Info("score +10 - partitioned gets successful")

	// Heal and wait
	err = k8sClient.DeleteNetPolicies(conf.Namespace, k8s.GroupLabels(conf.GroupName))
	if err != nil {
		log.Errorf("failed to heal partition: %v", err)
		return score
	}
	time.Sleep(11 * time.Second)

	var keyCount int
	keyCount, _, cm, statusCode, err = kvs3client.GetKeyList(addresses[0], cm)
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
	if keyCount != 2*conf.NumKeys {
		log.WithFields(logrus.Fields{
			"expected": 2 * conf.NumKeys,
			"received": keyCount,
		}).Error("invalid key count for get key list")
		return score
	}

	score += 10
	log.Info("score +10 - key count after network heal successful")

	for i := 0; i < conf.NumKeys; i++ {
		for b := 0; b < 2; b++ {
			var value string
			value, cm, statusCode, err = kvs3client.GetKey(
				batches[b][0],
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
	}

	score += 10
	log.Info("score +10 - causal ordering after network heal successful")

	for i := conf.NumKeys; i < 2*conf.NumKeys; i++ {
		var expectedVal string
		for b := 0; b < 2; b++ {
			var value string
			value, cm, statusCode, err = kvs3client.GetKey(
				batches[(b+i)%2][0],
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
			if b == 0 {
				expectedVal = value
				continue
			}

			if value != expectedVal {
				log.WithFields(logrus.Fields{
					"expected": expectedVal,
					"received": value,
				}).Error("invalid value - bad tie-breakink")
				return score
			}
		}
	}

	score += 10
	log.Info("score +10 - tie-breaking after network heal successful")

	return score
}
