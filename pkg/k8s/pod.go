package k8s

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1 "k8s.io/client-go/applyconfigurations/core/v1"
	applymetav1 "k8s.io/client-go/applyconfigurations/meta/v1"
)

const PodPort = "8080"

func (c *Client) ListPods(ns string, labels map[string]string) (*v1.PodList, error) {
	c.LazyInit()
	return c.Clientset.CoreV1().Pods(ns).List(context.TODO(), metav1.ListOptions{
		LabelSelector: condenseLabelsMap(labels),
	})
}

type PodMetaDetails struct {
	Ip    string
	Batch int
	Index int
}

func (c *Client) ListAddressGroupIndexMappings(
	ns string, labels map[string]string,
) (map[string]PodMetaDetails, error) {
	c.LazyInit()
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()

	res := make(map[string]PodMetaDetails)
	for {
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
			for _, l := range []string{BatchKey, IndexKey} {
				if _, ok := pod.ObjectMeta.Labels[l]; !ok {
					return nil, fmt.Errorf("pod has no label with key=%s", l)
				}
			}
			podInfo := PodMetaDetails{
				Ip:    pod.Status.PodIP,
				Batch: IntFromIntLabel(pod.ObjectMeta.Labels[BatchKey]),
				Index: IntFromIntLabel(pod.ObjectMeta.Labels[IndexKey]),
			}
			res[fmt.Sprintf("%s:%s", pod.Status.PodIP, PodPort)] = podInfo
		}
		if ready {
			break
		}
	}
	return res, nil
}

func PodAddrsFromMappings(m map[string]PodMetaDetails) []string {
	var res []string
	for k, _ := range m {
		res = append(res, k)
	}
	sort.Strings(res)
	return res
}

func (c *Client) ListPodAddresses(ns string, labels map[string]string) ([]string, error) {
	c.LazyInit()
	m, err := c.ListAddressGroupIndexMappings(ns, labels)
	if err != nil {
		return nil, err
	}
	return PodAddrsFromMappings(m), nil
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
	addressEnvValue := fmt.Sprintf("$(POD_IP):%s", PodPort)
	imagePullPolicy := v1.PullAlways
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
					Name:            &containerName,
					Image:           &image,
					ImagePullPolicy: &imagePullPolicy,
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

func (c *Client) AwaitDeletion(ns string, labels map[string]string) error {
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()

	for {
		select {
		case <-deadline.C:
			return fmt.Errorf("deadline for pod deletion exceeded; ns=%s; labels=%v", ns, labels)
		default: // Fall through
		}

		pods, err := c.ListPods(ns, labels)
		if err != nil {
			return err
		}
		if len(pods.Items) == 0 {
			return nil
		}
	}
}

func (c *Client) GetPodLogs(ns string, labels map[string]string) ([]string, error) {
	pods, err := c.ListPods(ns, labels)
	if err != nil {
		return nil, err
	}
	var res []string
	for _, pod := range pods.Items {
		podLogOpts := v1.PodLogOptions{}
		logs := c.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &podLogOpts)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		podLogs, err := logs.Stream(ctx)
		if err != nil {
			return nil, fmt.Errorf("error in opening log stream: %w", err)
		}
		defer podLogs.Close()

		buf := new(bytes.Buffer)
		_, err = io.Copy(buf, podLogs)
		if err != nil {
			return nil, fmt.Errorf("error in copying pod log stream to buf: %w", err)
		}
		str := buf.String()

		res = append(res, str)
	}
	return res, nil
}
