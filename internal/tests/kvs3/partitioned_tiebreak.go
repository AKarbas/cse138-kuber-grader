package kvs3

import (
	"fmt"
	"sort"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/AKarbas/cse138-kuber-grader/pkg/k8s"
	"github.com/AKarbas/cse138-kuber-grader/pkg/kvs3client"
)

const (
	PartitionedTotalOrderMaxScore = 40
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

	success := true
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

	partitionCms := make([]kvs3client.CausalMetadata, 2)

	// First batch of causal puts
	for i := 0; i < conf.NumKeys; i++ {
		batchId := i % 2
		partitionCms[batchId], statusCode, err = kvs3client.PutKeyVal(
			batches[batchId][i%conf.NumNodes],
			key(i),
			val(i, 0),
			partitionCms[batchId],
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
			}).Warn("invalid status code for put")
			success = false
		}
	}

	// Second batch of causal puts
	for i := 0; i < conf.NumKeys; i++ {
		batchId := (i + 1) % 2
		partitionCms[batchId], statusCode, err = kvs3client.PutKeyVal(
			batches[(i+1)%2][i%conf.NumNodes],
			key(i),
			val(i, 1),
			partitionCms[batchId],
		)
		if err != nil {
			log.Errorf("failed to put key-val: %v", err)
			return score
		}
		if statusCode != 200 && statusCode != 201 {
			log.WithFields(logrus.Fields{
				"expected": "200|201",
				"received": statusCode,
			}).Warn("invalid status code for put")
			success = false
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
			}).Warn("invalid status code for put")
			success = false
		}
	}

	if success {
		score += 10
		log.Info("score +10 - partitioned puts successful")
	}

	// Test correct/stall reads
	success = true
	for b := 0; b < 2; b++ {
		for cmIdx := 0; cmIdx < 2; cmIdx++ {
			i := conf.NumKeys - 1
			var value string
			value, _, statusCode, err = kvs3client.GetKey(
				batches[b][i%conf.NumNodes],
				key(i),
				partitionCms[cmIdx],
			)
			if err != nil {
				log.Errorf("failed to get key: %v", err)
				return score
			}
			if b == cmIdx && statusCode != 200 {
				log.WithFields(logrus.Fields{
					"expected": "200",
					"received": statusCode,
				}).Warn("invalid status code for get (node/partition has the entire causal history of the request)")
				success = false
			} else if b != cmIdx && statusCode != 200 && statusCode != 500 {
				log.WithFields(logrus.Fields{
					"expected": "200|500",
					"received": statusCode,
				}).Warn("invalid status code for get")
				success = false
			}
			expected0 := val(i, 0)
			expected1 := val(i, 1)
			if statusCode == 200 && value != expected0 && value != expected1 {
				log.WithFields(logrus.Fields{
					"expected": fmt.Sprintf("%s|%s", expected0, expected1),
					"received": value,
				}).Warn("invalid value")
				success = false
			}
		}
	}

	if success {
		score += 10
		log.Info("score +10 - partitioned gets successful")
	}

	// Heal and wait
	err = k8sClient.DeleteNetPolicies(conf.Namespace, k8s.GroupLabels(conf.GroupName))
	if err != nil {
		log.Errorf("failed to heal partition: %v", err)
		return score
	}
	time.Sleep(11 * time.Second)

	success = true
	var keyCount int
	keyCount, _, _, statusCode, err = kvs3client.GetKeyList(addresses[0], nil)
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
	if keyCount != 2*conf.NumKeys {
		log.WithFields(logrus.Fields{
			"expected": 2 * conf.NumKeys,
			"received": keyCount,
		}).Warn("invalid key count for get key list")
		success = false
	}

	if success {
		score += 10
		log.Info("score +10 - key count after network heal successful")
	}

	success = true
	for i := 0; i < 2*conf.NumKeys; i++ {
		var expectedVal string
		for b := 0; b < 2; b++ {
			var value string
			value, _, statusCode, err = kvs3client.GetKey(
				batches[(b+i)%2][0],
				key(i),
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

			expected0 := val(i, 0)
			expected1 := val(i, 1)
			if value != expected0 && value != expected1 {
				log.WithFields(logrus.Fields{
					"expected": fmt.Sprintf("%s|%s", expected0, expected1),
					"received": value,
				}).Warn("invalid value")
				success = false
			}

			if b == 0 {
				expectedVal = value
				continue
			}

			if value != expectedVal {
				log.WithFields(logrus.Fields{
					"expected": expectedVal,
					"received": value,
				}).Warn("invalid value - bad tie-breaking")
				success = false
			}
		}
	}

	if success {
		score += 10
		log.Info("score +10 - tie-breaking after network heal successful")
	}

	return score
}
