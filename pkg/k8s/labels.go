package k8s

import (
	"fmt"
	"strings"
)

func GroupLabels(groupName string) map[string]string {
	res := make(map[string]string)
	res["group"] = groupName
	return res
}

func BatchLabels(groupName string, batch int) map[string]string {
	res := GroupLabels(groupName)
	res["batch"] = fmt.Sprintf("%d", batch)
	return res
}

func PodLabels(groupName string, batch, idx int) map[string]string {
	res := BatchLabels(groupName, batch)
	res["index"] = fmt.Sprintf("%d", idx)
	return res
}

func PodLabelsNoBatch(groupName string, idx int) map[string]string {
	res := GroupLabels(groupName)
	res["index"] = fmt.Sprintf("%d", idx)
	return res
}

func condenseLabelsMap(lm map[string]string) string {
	res := strings.Builder{}
	for k, v := range lm {
		if res.Len() > 0 {
			res.WriteString(",")
		}
		res.WriteString(k)
		res.WriteString("=")
		res.WriteString(v)
	}
	return res.String()
}
