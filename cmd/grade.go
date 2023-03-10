package main

import (
	"fmt"
	"time"

	"github.com/AKarbas/cse138-kuber-grader/pkg/k8s"
)

func main() {
	client := k8s.Client{}

	for {
		pods, err := client.ListPods("", map[string]string{"app": "hello"})
		if err != nil {
			panic(err.Error())
		}
		fmt.Printf("There are %d pods in the cluster\n", len(pods.Items))

		for idx, pod := range pods.Items {
			fmt.Printf("Index %d: name: %s\n", idx, pod.GetName())
		}

		addresses, err := client.ListPodAddresses("", map[string]string{"app": "hello"})
		if err != nil {
			panic(err.Error())
		}
		for idx, addr := range addresses {
			fmt.Printf("Pod %d: %s\n", idx+1, addr)
		}
		time.Sleep(10 * time.Second)
	}
}
