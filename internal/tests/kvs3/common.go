package kvs3

import "fmt"

func key(i int) string {
	return fmt.Sprintf("Key-%d", i)
}

func val(i, j int) string {
	return fmt.Sprintf("Val-%d-%d", i, j)
}
