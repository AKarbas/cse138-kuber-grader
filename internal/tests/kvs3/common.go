package kvs3

import "fmt"

type TestFunc func(config TestConfig) int

func key(i int) string {
	return fmt.Sprintf("Key-%d", i)
}

func val(i, j int) string {
	return fmt.Sprintf("Val-%d-%d", i, j)
}
