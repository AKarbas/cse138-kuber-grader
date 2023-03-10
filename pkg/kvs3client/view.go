package kvs3client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type View struct {
	Nodes []string `json:"view"`
}

var ViewHttpClient = http.DefaultClient

func KvsAdminViewUrl(addr string) string {
	return fmt.Sprintf("http://%s/kvs/admin/view", addr)
}

func PutView(dest string, nodes []string) (int, error) {
	data, err := json.Marshal(View{Nodes: nodes})
	if err != nil {
		panic(err.Error())
	}

	req, err := http.NewRequest(http.MethodPut, KvsAdminViewUrl(dest), bytes.NewBuffer(data))
	if err != nil {
		panic(err.Error())
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := ViewHttpClient.Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

func GetView(dest string) ([]string, int, error) {
	resp, err := ViewHttpClient.Get(KvsAdminViewUrl(dest))
	if err != nil {
		return nil, 0, err
	}
	res := View{}
	err = json.NewDecoder(resp.Body).Decode(&res)
	resp.Body.Close()
	return res.Nodes, resp.StatusCode, err
}

func DeleteView(dest string) (int, error) {
	req, err := http.NewRequest(http.MethodDelete, KvsAdminViewUrl(dest), nil)
	if err != nil {
		panic(err.Error())
	}
	resp, err := ViewHttpClient.Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}
