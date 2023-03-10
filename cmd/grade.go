package main

import "github.com/AKarbas/cse138-kuber-grader/internal/tests/kvs3"

func main() {
	conf := kvs3.TestConfig{
		Registry:  "localhost:32000",
		ImageTag:  "cse138-hw3-v1.0",
		Namespace: "default",
		GroupName: "groupname",
		NumNodes:  3,
		NumKeys:   3,
	}

	kvs3.BasicKVTest(conf)
}
