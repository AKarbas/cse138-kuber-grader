package kvs4client

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/AKarbas/cse138-kuber-grader/pkg/kvs3client"
)

type CausalMetadata = kvs3client.CausalMetadata
type BaseBody = kvs3client.BaseBody
type KeyListBody struct {
	kvs3client.KeyListBody
	ShardId int64 `json:"shard_id"`
}

var PutKeyVal = kvs3client.PutKeyVal
var GetKey = kvs3client.GetKey
var DeleteKey = kvs3client.DeleteKey
var KvsDataUrl = kvs3client.KvsDataUrl

var dataHttpClient = http.Client{
	Timeout: 25 * time.Second,
}

func GetKeyList(dest string, cm CausalMetadata) (KeyListBody, int, error) {
	data, err := json.Marshal(BaseBody{CM: cm})
	if err != nil {
		panic(err.Error())
	}

	req, err := http.NewRequest(http.MethodGet, KvsDataUrl(dest), bytes.NewBuffer(data))
	if err != nil {
		panic(err.Error())
	}
	req.Header.Set("Content-Type", "application/json")

	res := KeyListBody{}
	resp, err := dataHttpClient.Do(req)
	if err != nil {
		return res, 0, err
	}

	err = json.NewDecoder(resp.Body).Decode(&res)
	resp.Body.Close()
	if err != nil {
		return res, 0, err
	}

	return res, resp.StatusCode, nil
}
