package k8s

import (
	"context"
	"fmt"

	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	applymetav1 "k8s.io/client-go/applyconfigurations/meta/v1"
	v1 "k8s.io/client-go/applyconfigurations/networking/v1"
)

func (c *Client) IsolateBatch(ns string, groupName string, batch int) error {
	c.LazyInit()
	return c.CreateNetPolicy(ns, fmt.Sprintf("%s-g%d", groupName, batch), BatchLabels(groupName, batch), nil)
}

func (c *Client) IsolatePodByIps(ns string, groupName string, idx int, ips []string) error {
	c.LazyInit()
	return c.CreateNetPolicy(ns, fmt.Sprintf("%s-p%d", groupName, idx), PodLabelsNoBatch(groupName, idx), ips)
}

func (c *Client) IsolatePod(ns string, groupName string, idx int) error {
	c.LazyInit()
	return c.CreateNetPolicy(ns, fmt.Sprintf("%s-p%d", groupName, idx), PodLabelsNoBatch(groupName, idx), nil)
}

func (c *Client) CreateNetPolicy(ns, name string, labels map[string]string, extraIps []string) error {
	c.LazyInit()
	kind := "NetworkPolicy"
	apiVersion := "networking.k8s.io/v1"
	//hostCidr := "192.168.0.0/16"
	from := []v1.NetworkPolicyPeerApplyConfiguration{
		{
			PodSelector: &applymetav1.LabelSelectorApplyConfiguration{
				MatchLabels: labels,
			},
		},
		//{
		//	IPBlock: &v1.IPBlockApplyConfiguration{
		//		CIDR: &hostCidr,
		//	},
		//},
	}
	for _, ip := range extraIps {
		ipCidr := fmt.Sprintf("%s/32", ip)
		from = append(from, v1.NetworkPolicyPeerApplyConfiguration{
			IPBlock: &v1.IPBlockApplyConfiguration{
				CIDR: &ipCidr,
			},
		})
	}

	req := &v1.NetworkPolicyApplyConfiguration{
		TypeMetaApplyConfiguration: applymetav1.TypeMetaApplyConfiguration{
			Kind:       &kind,
			APIVersion: &apiVersion,
		},
		ObjectMetaApplyConfiguration: &applymetav1.ObjectMetaApplyConfiguration{
			Name:      &name,
			Namespace: &ns,
			Labels:    labels,
		},
		Spec: &v1.NetworkPolicySpecApplyConfiguration{
			PodSelector: &applymetav1.LabelSelectorApplyConfiguration{
				MatchLabels: labels,
			},
			Ingress: []v1.NetworkPolicyIngressRuleApplyConfiguration{
				{
					From: from,
				},
			},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
		},
	}

	applyOpts := metav1.ApplyOptions{
		FieldManager: kFieldManager,
		Force:        true,
	}
	_, err := c.NetworkingV1().NetworkPolicies(ns).Apply(context.TODO(), req, applyOpts)
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
