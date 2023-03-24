package kvs4client

import (
	"bytes"
	"encoding/json"
	"net/http"
	"sort"
	"time"

	"github.com/AKarbas/cse138-kuber-grader/pkg/kvs3client"
)

type ViewReq struct {
	Nodes     []string `json:"nodes"`
	NumShards int      `json:"num_shards"`
}

type ViewResp struct {
	View []ViewRespShard `json:"view'"`
}

type ViewRespShard struct {
	ShardId int64    `json:"shard_id"`
	Nodes   []string `json:"nodes"`
}

var viewHttpClient = http.Client{
	Timeout: 25 * time.Second,
}

var KvsAdminViewUrl = kvs3client.KvsAdminViewUrl

func PutView(dest string, view ViewReq) (int, error) {
	data, err := json.Marshal(view)
	if err != nil {
		panic(err.Error())
	}

	req, err := http.NewRequest(http.MethodPut, KvsAdminViewUrl(dest), bytes.NewBuffer(data))
	if err != nil {
		panic(err.Error())
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := viewHttpClient.Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

func GetView(dest string) (ViewResp, int, error) {
	res := ViewResp{}
	resp, err := viewHttpClient.Get(KvsAdminViewUrl(dest))
	if err != nil {
		return res, 0, err
	}
	err = json.NewDecoder(resp.Body).Decode(&res)
	resp.Body.Close()
	if err != nil {
		return res, resp.StatusCode, err
	}
	for _, vrs := range res.View {
		sort.Strings(vrs.Nodes)
	}
	return res, resp.StatusCode, nil
}

func DeleteView(dest string) (int, error) {
	req, err := http.NewRequest(http.MethodDelete, KvsAdminViewUrl(dest), nil)
	if err != nil {
		panic(err.Error())
	}
	resp, err := viewHttpClient.Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}
