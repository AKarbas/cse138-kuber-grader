package kvs3

import "fmt"

type TestConfig struct {
	Registry  string
	ImageTag  string
	Namespace string
	GroupName string
	NumNodes  int
	NumKeys   int
}

func (tc TestConfig) Image() string {
	if tc.Registry == "" {
		return fmt.Sprintf("%s:%s", tc.GroupName, tc.ImageTag)
	}
	return fmt.Sprintf("%s/%s:%s", tc.Registry, tc.GroupName, tc.ImageTag)
}
