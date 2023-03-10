package k8s

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const kPodPort = "8000"

func (c *Client) ListPods(ns string, labels map[string]string) (*v1.PodList, error) {
	c.LazyInit()
	return c.Clientset.CoreV1().Pods(ns).List(context.TODO(), metav1.ListOptions{
		LabelSelector: condenseLabelsMap(labels),
	})
}

func (c *Client) ListPodAddresses(ns string, labels map[string]string) ([]string, error) {
	c.LazyInit()
	pods, err := c.ListPods(ns, labels)
	if err != nil {
		return nil, err
	}
	var res []string
	for _, pod := range pods.Items {
		res = append(res, fmt.Sprintf("%s:%s", pod.Status.PodIP, kPodPort))
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
	req := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    labels,
		},
		Spec: v1.PodSpec{
			RestartPolicy: "Never",
			Containers: []v1.Container{
				{
					Name:  "main",
					Image: image,
					Env: []v1.EnvVar{
						{
							Name: "POD_IP",
							ValueFrom: &v1.EnvVarSource{
								FieldRef: &v1.ObjectFieldSelector{
									FieldPath: "status.podIP",
								},
							},
						},
						{
							Name:  "ADDRESS",
							Value: fmt.Sprintf("$(POD_IP):%s", kPodPort),
						},
					},
				},
			},
		},
	}

	_, err := c.Clientset.CoreV1().Pods(ns).Create(context.TODO(), req, metav1.CreateOptions{})
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
