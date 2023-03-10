package main

import (
	"fmt"
	"time"

	"github.com/AKarbas/cse138-kuber-grader/pkg/k8s"
)

func main() {
	client := k8s.Client{}

	for {
		pods, err := client.ListPods("", "app=hello")
		if err != nil {
			panic(err.Error())
		}
		fmt.Printf("There are %d pods in the cluster\n", len(pods.Items))

		for idx, pod := range pods.Items {
			fmt.Printf("Index %d: name: %s\n", idx, pod.GetName())
		}
		time.Sleep(10 * time.Second)
	}
}
