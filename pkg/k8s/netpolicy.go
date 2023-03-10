package k8s

import (
	"context"
	"fmt"

	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (c *Client) IsolateBatch(ns string, groupName string, batch int) error {
	c.LazyInit()
	return c.CreateNetPolicy(ns, fmt.Sprintf("%s-g%d", groupName, batch), GroupLabels(groupName))
}

func (c *Client) IsolatePod(ns string, groupName string, idx int) error {
	c.LazyInit()
	return c.CreateNetPolicy(ns, fmt.Sprintf("%s-p%d", groupName, idx), PodLabelsNoBatch(groupName, idx))
}

func (c *Client) CreateNetPolicy(ns, name string, labels map[string]string) error {
	c.LazyInit()
	req := &networkingv1.NetworkPolicy{
		TypeMeta: metav1.TypeMeta{
			Kind:       "NetworkPolicy",
			APIVersion: "networking.k8s.io/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    labels,
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: labels,
			},
			Ingress: []networkingv1.NetworkPolicyIngressRule{
				{
					From: []networkingv1.NetworkPolicyPeer{
						{
							PodSelector: &metav1.LabelSelector{
								MatchLabels: labels,
							},
						},
					},
				},
			},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
		},
	}

	_, err := c.NetworkingV1().NetworkPolicies(ns).Create(context.TODO(), req, metav1.CreateOptions{})
	return err
}

func (c *Client) DeleteNetPolicies(ns string, labels map[string]string) error {
	c.LazyInit()
	netPolicies, err := c.NetworkingV1().NetworkPolicies(ns).List(
		context.TODO(), metav1.ListOptions{LabelSelector: condenseLabelsMap(labels)})
	if err != nil {
		return err
	}
	for _, np := range netPolicies.Items {
		err := c.NetworkingV1().NetworkPolicies(ns).Delete(context.TODO(), np.GetName(), metav1.DeleteOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}
