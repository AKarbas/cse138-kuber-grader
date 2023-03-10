package k8s

import (
	"context"
	"flag"
	"fmt"
	"path/filepath"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

const kPodPort = "8000"

type Client struct {
	*kubernetes.Clientset
}

func (c *Client) LazyInit() {
	if c.Clientset != nil {
		return
	}

	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	// create the clientset
	c.Clientset, err = kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
}

func GroupLabels(groupName string) map[string]string {
	res := make(map[string]string)
	res["group"] = groupName
	return res
}

func BatchLabels(groupName string, batch int) map[string]string {
	res := GroupLabels(groupName)
	res["batch"] = fmt.Sprintf("%d", batch)
	return res
}

func PodLabels(groupName string, batch, idx int) map[string]string {
	res := BatchLabels(groupName, batch)
	res["index"] = fmt.Sprintf("%d", idx)
	return res
}

func condenseLabelsMap(lm map[string]string) string {
	res := strings.Builder{}
	for k, v := range lm {
		if res.Len() > 0 {
			res.WriteString(",")
		}
		res.WriteString(k)
		res.WriteString("=")
		res.WriteString(v)
	}
	return res.String()
}

func (c *Client) ListPods(ns string, labels map[string]string) (*v1.PodList, error) {
	c.LazyInit()
	return c.Clientset.CoreV1().Pods(ns).List(context.TODO(), metav1.ListOptions{
		LabelSelector: condenseLabelsMap(labels),
	})
}

func (c *Client) ListPodAddresses(ns string, labels map[string]string) ([]string, error) {
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

func (c *Client) StartPods(ns, groupName, image string, batches, perBatch int) error {
	c.LazyInit()
	errChan := make(chan error, 1)
	defer close(errChan)
	for i := 1; i <= batches; i++ {
		for j := 1; j <= perBatch; j++ {
			go func(i, j int) {
				req := &v1.Pod{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      fmt.Sprintf("%s-b%d-p%d", groupName, i, j),
						Namespace: ns,
						Labels:    PodLabels(groupName, i, ((i-1)*batches)+j),
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
