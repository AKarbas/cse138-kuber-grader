package kvs4

import (
	"fmt"

	"github.com/AKarbas/cse138-kuber-grader/pkg/k8s"
)

type ViewConfig struct {
	NumNodes  int
	NumShards int
}

func (vc ViewConfig) String() string {
	return fmt.Sprintf("NumNodes: %d, NumShards: %d", vc.NumNodes, vc.NumShards)
}

type Config struct {
	Registry  string
	GroupName string
	ImageTag  string
	Namespace string
}

func (c Config) Image() string {
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
}
