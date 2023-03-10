package kvs3client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type CausalMetadata interface{}

var kEmptyCM = struct{}{}

type BaseBody struct {
	CM CausalMetadata `json:"causal-metadata"`
}

type ValBody struct {
	BaseBody `json:",inline"`
	Val      string `json:"val"`
}

type KeyListBody struct {
	BaseBody `json:",inline"`
	Count    int      `json:"count"`
	Keys     []string `json:"keys"`
}

var dataHttpClient = http.Client{
	Timeout: 21 * time.Second,
}

func KvsDataKeyUrl(addr, key string) string {
	return fmt.Sprintf("http://%s/kvs/data/%s", addr, strings.ReplaceAll(key, " ", "-"))
}

func KvsDataUrl(addr string) string {
	return fmt.Sprintf("http://%s/kvs/data", addr)
}

func parseBaseBody(r *http.Response) (BaseBody, error) {
	res := BaseBody{}
	err := json.NewDecoder(r.Body).Decode(&res)
	return res, err
}

func PutKeyVal(dest, key, val string, cm CausalMetadata) (CausalMetadata, int, error) {
	if cm == nil {
		cm = kEmptyCM
	}
	data, err := json.Marshal(ValBody{
		BaseBody: BaseBody{CM: cm},
		Val:      val,
	})
	if err != nil {
		panic(err.Error())
	}

	req, err := http.NewRequest(http.MethodPut, KvsDataKeyUrl(dest, key), bytes.NewBuffer(data))
	if err != nil {
		panic(err.Error())
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := dataHttpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}

	res, err := parseBaseBody(resp)
	resp.Body.Close()
	if err != nil {
		return nil, 0, err
	}

	return res.CM, resp.StatusCode, nil
}

func GetKey(dest, key string, cm CausalMetadata) (string, CausalMetadata, int, error) {
	if cm == nil {
		cm = kEmptyCM
	}
	data, err := json.Marshal(BaseBody{CM: cm})
	if err != nil {
		panic(err.Error())
	}

	req, err := http.NewRequest(http.MethodGet, KvsDataKeyUrl(dest, key), bytes.NewBuffer(data))
	if err != nil {
		panic(err.Error())
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := dataHttpClient.Do(req)
	if err != nil {
		return "", nil, 0, err
	}

	res := ValBody{}
	err = json.NewDecoder(resp.Body).Decode(&res)
	resp.Body.Close()
	if err != nil {
		return "", nil, 0, err
	}

	return res.Val, res.CM, resp.StatusCode, nil
}

func DeleteKey(dest, key string, cm CausalMetadata) (CausalMetadata, int, error) {
	if cm == nil {
		cm = kEmptyCM
	}
	data, err := json.Marshal(BaseBody{CM: cm})
	if err != nil {
		panic(err.Error())
	}

	req, err := http.NewRequest(http.MethodDelete, KvsDataKeyUrl(dest, key), bytes.NewBuffer(data))
	if err != nil {
		panic(err.Error())
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := dataHttpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}

	res, err := parseBaseBody(resp)
	resp.Body.Close()
	if err != nil {
		return nil, 0, err
	}

	return res.CM, resp.StatusCode, nil
}

func GetKeyList(dest string, cm CausalMetadata) (int, []string, CausalMetadata, int, error) {
	if cm == nil {
		cm = kEmptyCM
	}
	data, err := json.Marshal(BaseBody{CM: cm})
	if err != nil {
		panic(err.Error())
	}

	req, err := http.NewRequest(http.MethodGet, KvsDataUrl(dest), bytes.NewBuffer(data))
	if err != nil {
		panic(err.Error())
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := dataHttpClient.Do(req)
	if err != nil {
		return 0, nil, nil, 0, err
	}

	res := KeyListBody{}
	err = json.NewDecoder(resp.Body).Decode(&res)
	resp.Body.Close()
	if err != nil {
		return 0, nil, nil, 0, err
	}

	return res.Count, res.Keys, res.CM, resp.StatusCode, nil
}
