package main

import (
	"github.com/AKarbas/cse138-kuber-grader/internal/tests/kvs3"
)

//var groups = []string{
//}

func main() {
	groupName := "groupname"

	//conf1 := kvs3.TestConfig{
	//	Registry:  "localhost:32000",
	//	ImageTag:  "cse138-hw3-v1.0",
	//	Namespace: "default",
	//	GroupName: groupName,
	//	NumNodes:  3,
	//	NumKeys:   3,
	//}
	//kvs3.BasicKVTest(conf1)

	conf2 := kvs3.TestConfig{
		Registry:  "localhost:32000",
		ImageTag:  "cse138-hw3-v1.0",
		Namespace: "default",
		GroupName: groupName,
		NumNodes:  1,
		NumKeys:   8,
	}
	kvs3.PartitionedTotalOrderTest(conf2)

	//wg := sync.WaitGroup{}
	//wg.Add(len(groups))
	//for _, group := range groups {
	//	go func(g string) {
	//		defer wg.Done()
	//		conf2 := kvs3.TestConfig{
	//			Registry:  "localhost:32000",
	//			ImageTag:  "cse138-hw3-v1.0",
	//			Namespace: "default",
	//			GroupName: g,
	//			NumNodes:  1,
	//			NumKeys:   8,
	//		}
	//		kvs3.PartitionedTotalOrderTest(conf2)
	//	}(group)
	//}
	//wg.Wait()
}
