package main

import (
	"fmt"
	"strings"

	"github.com/AKarbas/cse138-kuber-grader/pkg/k8s"
	"github.com/AKarbas/cse138-kuber-grader/pkg/kvs3client"
)

func main() {
	client := k8s.Client{}
	ns := "default"
	group := "groupname"
	group = strings.ToLower(strings.TrimSpace(group))
	//image := fmt.Sprintf("localhost:32000/%s:cse138-hw3-v1.0", group)
	//client.DeletePods(ns, k8s.GroupLabels(group))
	err := client.CreatePods(ns, group, image, 3, 3)
	//if err != nil {
	//	panic(err.Error())
	//}
	addrs, err := client.ListPodAddresses(ns, k8s.GroupLabels(group))
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(addrs)

	//stt, err := kvs3client.PutView(addrs[0], addrs)
	//if err != nil {
	//	panic(err.Error())
	//}
	//fmt.Println(stt)

	view, stt, err := kvs3client.GetView(addrs[0])
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(stt)
	fmt.Println(view)
}
