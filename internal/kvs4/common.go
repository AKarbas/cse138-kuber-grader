package kvs4

import (
	"fmt"
	"reflect"

	"k8s.io/utils/strings/slices"

	"github.com/AKarbas/cse138-kuber-grader/pkg/k8s"
	"github.com/AKarbas/cse138-kuber-grader/pkg/kvs4client"
)

type ViewConfig struct {
	NumNodes  int
	NumShards int
}

func (vc ViewConfig) String() string {
	return fmt.Sprintf("NumNodes: %d, NumShards: %d", vc.NumNodes, vc.NumShards)
}

type TestConfig struct {
	Registry  string
	GroupName string
	ImageTag  string
	Namespace string
}

func (c TestConfig) Image() string {
	if c.Registry == "" {
		return fmt.Sprintf("%s:%s", c.GroupName, c.ImageTag)
	}
	return fmt.Sprintf("%s/%s:%s", c.Registry, c.GroupName, c.ImageTag)
}

func PreTestCleanup(kc k8s.Client, namespace, groupName string) error {
	if err := kc.DeletePods(namespace, k8s.GroupLabels(groupName)); err != nil {
		return fmt.Errorf("failed to delete pods: %w", err)
	}
	if err := kc.AwaitDeletion(namespace, k8s.GroupLabels(groupName)); err != nil {
		return fmt.Errorf("failed when awaiting deletion of pods: %w", err)
	}
	if err := kc.DeleteNetPolicies(namespace, k8s.GroupLabels(groupName)); err != nil {
		return fmt.Errorf("failed to delete network policies: %w", err)
	}
	return nil
}

func PostTestCleanup(kc k8s.Client, namespace, groupName string) {
	_ = kc.DeletePods(namespace, k8s.GroupLabels(groupName))
	_ = kc.AwaitDeletion(namespace, k8s.GroupLabels(groupName))
	_ = kc.DeleteNetPolicies(namespace, k8s.GroupLabels(groupName))
}

func TestViewsConsistent(addresses []string, conf ViewConfig) (kvs4client.ViewResp, error) {
	firstView := kvs4client.ViewResp{}
	for idx, addr := range addresses {
		resp, statusCode, err := kvs4client.GetView(addr)
		if err != nil {
			return kvs4client.ViewResp{}, fmt.Errorf("failed to get view from node %s: %w", addr, err)
		}
		if statusCode != 200 {
			return kvs4client.ViewResp{}, fmt.Errorf("got bad status code when getting view from node %s, expected 200 but got %d",
				addr, statusCode)
		}

		if idx == 0 {
			if err := ValidateView(resp, conf); err != nil {
				return kvs4client.ViewResp{}, fmt.Errorf("received bad view from first node: %w", err)
			}
			firstView = resp
			continue
		}

		if !reflect.DeepEqual(resp, firstView) {
			return kvs4client.ViewResp{}, fmt.Errorf("received view from node %d (%s) different from view received from node 1, "+
				"received view: %+v, expected view: %+v", idx+1, addr, resp, firstView)
		}
	}
	return firstView, nil
}

func ValidateView(viewResp kvs4client.ViewResp, conf ViewConfig) error {

	minNodesPerShard := conf.NumNodes / conf.NumShards
	maxNodesPerShard := (conf.NumNodes + conf.NumShards - 1) / conf.NumShards

	nodeCounter := 0
	for _, shard := range viewResp.View {
		shardNodes := len(shard.Nodes)
		if shardNodes < minNodesPerShard || shardNodes > maxNodesPerShard {
			return fmt.Errorf("invalid number of nodes in shard; n=%d, s=%d, shardNodes=%v "+
				"(expected: %d <= shardNodes <= %d)",
				conf.NumNodes, conf.NumShards, shard.Nodes, minNodesPerShard, maxNodesPerShard)
		}
		nodeCounter += shardNodes
	}

	if nodeCounter != conf.NumNodes {
		return fmt.Errorf("expected n=%d nodes in view but got %d; view=%+v", conf.NumNodes, nodeCounter, viewResp)
	}

	return nil
}

func Key(i int) string    { return fmt.Sprintf("key-%d", i) }
func Val(i, j int) string { return fmt.Sprintf("val-%d-%d", i, j) }

type SprayConfig struct {
	addresses              []string
	minI, maxI, minJ, maxJ int
	cm                     kvs4client.CausalMetadata
	noCm                   bool
	acceptedStatusCodes    []int
}

func SprayPuts(conf SprayConfig) (kvs4client.CausalMetadata, error) {
	cm := conf.cm
	for i := conf.minI; i <= conf.maxI; i++ {
		for j := conf.minJ; j <= conf.maxJ; j++ {
			nodeIdx := (i + j) % len(conf.addresses)
			key := Key(i)
			val := Val(i, j)
			errorDetails := fmt.Sprintf("failed to put key %s and val %s to node %s with CM from last access",
				key, val, conf.addresses[nodeIdx])
			if conf.noCm {
				errorDetails = fmt.Sprintf("failed to put key %s and val %s to node %s with CM={}",
					key, val, conf.addresses[nodeIdx])
			}
			if conf.noCm {
				cm = nil
			}
			var statusCode int
			var err error
			cm, statusCode, err = kvs4client.PutKeyVal(conf.addresses[nodeIdx], key, val, cm)
			if err != nil {
				return nil, fmt.Errorf("%s, got error: %v", errorDetails, err)
			}
			if !contains(conf.acceptedStatusCodes, statusCode) {
				return nil, fmt.Errorf("%s, expected status code in %v but got %d",
					errorDetails, conf.acceptedStatusCodes, statusCode)
			}
		}
	}

	if conf.noCm {
		cm = nil
	}
	return cm, nil
}

func contains(list []int, val int) bool {
	for _, x := range list {
		if val == x {
			return true
		}
	}
	return false
}

func SprayGets(conf SprayConfig) (kvs4client.CausalMetadata, error) {
	cm := conf.cm
	receivedVals := make(map[string]string)
	for i := conf.minI; i <= conf.maxI; i++ {
		var acceptedVals []string
		for j := conf.minJ; j <= conf.maxJ; j++ {
			acceptedVals = append(acceptedVals, Val(i, j))
		}
		if contains(conf.acceptedStatusCodes, 500) {
			acceptedVals = append(acceptedVals, "")
		}
		nodeIdx := i % len(conf.addresses)
		key := Key(i)
		errorDetails := fmt.Sprintf("failed to get key %s from node %s with CM from last access (expecting val in %v)",
			key, conf.addresses[nodeIdx], acceptedVals)
		if conf.noCm {
			errorDetails = fmt.Sprintf("failed to get key %s from node %s with CM={} (expecting val in %v)",
				key, conf.addresses[nodeIdx], acceptedVals)
		}
		if conf.noCm {
			cm = nil
		}
		var val string
		var statusCode int
		var err error
		val, cm, statusCode, err = kvs4client.GetKey(conf.addresses[nodeIdx], key, cm)
		if err != nil {
			return nil, fmt.Errorf("%s, got error: %v", errorDetails, err)
		}
		if !contains(conf.acceptedStatusCodes, statusCode) {
			return nil, fmt.Errorf("%s, expected status code in %v but got %d",
				errorDetails, conf.acceptedStatusCodes, statusCode)
		}
		if !slices.Contains(acceptedVals, val) {
			return nil, fmt.Errorf("%s, val=%s not in accepted vals", errorDetails, val)
		}
		if prevVal, ok := receivedVals[key]; !ok {
			receivedVals[key] = val
		} else if val != prevVal {
			return nil, fmt.Errorf("%s, got inconsistent values %s and %s for key %s",
				errorDetails, prevVal, val, key)
		}
	}

	if conf.noCm {
		cm = nil
	}
	return cm, nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func GenPartitions(v kvs4client.ViewResp) [][]string {
	numParts := 0
	for _, s := range v.View {
		if numParts == 0 || numParts > len(s.Nodes) {
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

func TestKeyLists(addresses []string, minI, maxI int) (map[string]map[string]struct{}, error) {
	shardKeys := make(map[string]map[string]struct{})
	exists := struct{}{}
	shardCounts := make(map[string]int)
	for idx, addr := range addresses {
		res, statusCode, err := kvs4client.GetKeyList(addr, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to get key list from node %d: %w", idx+1, err)
		}
		if statusCode != 200 {
			return nil, fmt.Errorf("bad status code for key list from node %d: %d (expected 200)", idx+1, 200)
		}
		if res.Count != len(res.Keys) {
			return nil, fmt.Errorf("bad key list: count (%d) != len(keys) (%d)", res.Count, len(res.Keys))
		}
		keySet := make(map[string]struct{})
		for _, k := range res.Keys {
			keySet[k] = exists
		}
		if len(keySet) != len(res.Keys) {
			return nil, fmt.Errorf("key list contains duplicates")
		}
		var prevKeys map[string]struct{}
		var ok bool
		if prevKeys, ok = shardKeys[res.ShardId]; !ok {
			shardKeys[res.ShardId] = keySet
			shardCounts[res.ShardId] = res.Count
			continue
		}
		if res.Count != shardCounts[res.ShardId] {
			return nil, fmt.Errorf("number of keys in shard inconsistent across nodes (got=%d, expected=%d, shardId=%s)",
				res.Count, shardCounts[res.ShardId], res.ShardId)
		}
		if !reflect.DeepEqual(keySet, prevKeys) {
			return nil, fmt.Errorf("keys in shard inconsistent across nodes (shardId=%s)", res.ShardId)
		}
	}

	totalCount := 0
	for _, cnt := range shardCounts {
		totalCount += cnt
	}
	if totalCount != (maxI - minI + 1) {
		return nil, fmt.Errorf("total number of keys in shards invalid, expected=%d, got=%d",
			maxI-minI+1, totalCount)
	}

	allKeys := make(map[string]struct{})
	for _, keySet := range shardKeys {
		for key, _ := range keySet {
			if _, ok := allKeys[key]; !ok {
				allKeys[key] = exists
				continue
			}
			return nil, fmt.Errorf("key %s exists in more than one shard", key)
		}
	}

	return shardKeys, nil
}

func NodeKeySets(
	shardKeySets map[string]map[string]struct{}, view kvs4client.ViewResp,
) (map[string]map[string]struct{}, error) {
	res := make(map[string]map[string]struct{})
	for _, shard := range view.View {
		for _, node := range shard.Nodes {
			var ok bool
			res[node], ok = shardKeySets[shard.ShardId]
			if !ok {
				return nil, fmt.Errorf("shardId (%s) not found in shardKeyLists, view=%+v", shard.ShardId, view)
			}
		}
	}
	return res, nil
}
