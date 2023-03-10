package k8s

import (
	"context"
	"flag"
	"fmt"
	"path/filepath"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

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

func (c *Client) ListPods(ns string, labels string) (*v1.PodList, error) {
	c.LazyInit()
	return c.Clientset.CoreV1().Pods(ns).List(context.TODO(), metav1.ListOptions{LabelSelector: labels})
}

func (c *Client) StartPods(ns, groupName, image string, groups, perGroup int) error {
	podPort := "8000"
	c.LazyInit()
	errChan := make(chan error, 1)
	defer close(errChan)
	for i := 1; i <= groups; i++ {
		for j := 1; j <= perGroup; j++ {
			go func(i, j int) {
				podLabels := make(map[string]string)
				podLabels["group"] = groupName
				podLabels["pod-group"] = fmt.Sprintf("%d", i)
				podLabels["pod-index"] = fmt.Sprintf("%d", j)

				req := &v1.Pod{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      fmt.Sprintf("%s-g%d-p%d", groupName, i, j),
						Namespace: ns,
						Labels:    podLabels,
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
										Value: fmt.Sprintf("$(POD_IP):%s", podPort),
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
	for e := range errChan {
		if e != nil {
			err = e
		}
	}
	return err
}
