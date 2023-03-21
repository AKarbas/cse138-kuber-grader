package k8s

import (
	"fmt"
	"strconv"
	"strings"
)

const GroupKey = "group"
const BatchKey = "batch"
const IndexKey = "index"

func GroupLabels(groupName string) map[string]string {
	res := make(map[string]string)
	res[GroupKey] = groupName
	return res
}

func BatchLabels(groupName string, batch int) map[string]string {
	res := GroupLabels(groupName)
	res[BatchKey] = fmt.Sprintf("%d", batch)
	return res
}

func PodLabels(groupName string, batch, idx int) map[string]string {
	res := BatchLabels(groupName, batch)
	res[IndexKey] = fmt.Sprintf("%d", idx)
	return res
}

func PodLabelsNoBatch(groupName string, idx int) map[string]string {
	res := GroupLabels(groupName)
	res[IndexKey] = fmt.Sprintf("%d", idx)
	return res
}

func IntFromIntLabel(il string) int {
	res, err := strconv.Atoi(il)
	if err != nil {
		panic(err)
	}
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
