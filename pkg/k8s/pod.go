package k8s

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1 "k8s.io/client-go/applyconfigurations/core/v1"
	applymetav1 "k8s.io/client-go/applyconfigurations/meta/v1"
)

const kPodPort = "8080"

func (c *Client) ListPods(ns string, labels map[string]string) (*v1.PodList, error) {
	c.LazyInit()
	return c.Clientset.CoreV1().Pods(ns).List(context.TODO(), metav1.ListOptions{
		LabelSelector: condenseLabelsMap(labels),
	})
}

func (c *Client) ListPodAddresses(ns string, labels map[string]string) ([]string, error) {
	c.LazyInit()
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()

	var res []string
	for true {
		select {
		case <-deadline.C:
			return nil, fmt.Errorf("deadline for pods ready exceeded; ns=%s; labels=%v", ns, labels)
		default: // Fall through
		}
		pods, err := c.ListPods(ns, labels)
		if err != nil {
			return nil, err
		}
		ready := true
		for _, pod := range pods.Items {
			if pod.Status.Phase != v1.PodRunning {
				ready = false
				break
			}
			res = append(res, fmt.Sprintf("%s:%s", pod.Status.PodIP, kPodPort))
		}
		if ready {
			break
		}
	}
	return res, nil
}

func (c *Client) CreatePods(ns, groupName, image string, batches, perBatch int) error {
	c.LazyInit()
	errChan := make(chan error, 1)
	defer close(errChan)
	for i := 1; i <= batches; i++ {
		for j := 1; j <= perBatch; j++ {
			go func(i, j int) {
				err := c.CreatePod(
					ns,
					fmt.Sprintf("%s-b%d-p%d", groupName, i, j),
					image, PodLabels(groupName, i, ((i-1)*batches)+j),
				)
				errChan <- err
			}(i, j)
		}
	}

	var err error
	for i := 0; i < batches*perBatch; i++ {
		e := <-errChan
		if e != nil {
			err = e
		}
	}
	return err
}

func (c *Client) CreatePod(ns, name, image string, labels map[string]string) error {
	c.LazyInit()
	kind := "Pod"
	apiVersion := "v1"
	restartPolicy := "Never"
	containerName := "main"
	podIpEnvName := "POD_IP"
	podIpFieldPath := "status.podIP"
	addressEnvName := "ADDRESS"
	addressEnvValue := fmt.Sprintf("$(POD_IP):%s", kPodPort)
	req := &corev1.PodApplyConfiguration{
		TypeMetaApplyConfiguration: applymetav1.TypeMetaApplyConfiguration{
			Kind:       &kind,
			APIVersion: &apiVersion,
		},
		ObjectMetaApplyConfiguration: &applymetav1.ObjectMetaApplyConfiguration{
			Name:      &name,
			Namespace: &ns,
			Labels:    labels,
		},
		Spec: &corev1.PodSpecApplyConfiguration{
			RestartPolicy: (*v1.RestartPolicy)(&restartPolicy),
			Containers: []corev1.ContainerApplyConfiguration{
				{
					Name:  &containerName,
					Image: &image,
					Env: []corev1.EnvVarApplyConfiguration{
						{
							Name: &podIpEnvName,
							ValueFrom: &corev1.EnvVarSourceApplyConfiguration{
								FieldRef: &corev1.ObjectFieldSelectorApplyConfiguration{
									FieldPath: &podIpFieldPath,
								},
							},
						},
						{
							Name:  &addressEnvName,
							Value: &addressEnvValue,
						},
					},
				},
			},
		},
	}

	applyOpts := metav1.ApplyOptions{
		FieldManager: kFieldManager,
		Force:        true,
	}
	_, err := c.Clientset.CoreV1().Pods(ns).Apply(context.TODO(), req, applyOpts)
	return err
}

func (c *Client) DeletePods(ns string, labels map[string]string) error {
	c.LazyInit()
	pods, err := c.Clientset.CoreV1().Pods(ns).List(
		context.TODO(), metav1.ListOptions{LabelSelector: condenseLabelsMap(labels)})
	if err != nil {
		return err
	}
	for _, pod := range pods.Items {
		err := c.Clientset.CoreV1().Pods(ns).Delete(context.TODO(), pod.GetName(), metav1.DeleteOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}
