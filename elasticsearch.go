package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"
)

type esClient struct {
	http     *http.Client
	endpoint string
}

func NewElasticsearchClient(endpoint string, httpClient *http.Client) *esClient {
	if httpClient == nil {
		timeout := 10 * time.Second
		httpClient = &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					Timeout: timeout,
				}).Dial,
				TLSHandshakeTimeout: timeout,
			},
		}
	}

	return &esClient{
		http:     httpClient,
		endpoint: endpoint,
	}
}

type shardInfo struct {
	Index            string `json:"index"`
	Shard            string `json:"shard"`
	PriRep           string `json:"prirep"`
	State            string `json:"state"`
	Docs             string `json:"docs"`
	Store            string `json:"store"`
	IP               string `json:"ip"`
	Node             string `json:"node"`
	UnassignedReason string `json:"unassigned.reason"`
}

func (s shardInfo) NumDocs() int {
	num, _ := strconv.Atoi(s.Docs)
	return num
}

func (s shardInfo) ShardID() int {
	id, _ := strconv.Atoi(s.Shard)
	return id
}

func (s shardInfo) IsPrimary() bool {
	return s.PriRep == "p"
}

func (s shardInfo) IsReplica() bool {
	return !s.IsPrimary()
}

func (es *esClient) CatShards() ([]shardInfo, error) {
	url := fmt.Sprintf("http://%s/_cat/shards?h=index,shard,prirep,state,docs,node,store,ip,unassigned.reason&format=json", es.endpoint)

	response, err := es.http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("request failed: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("cluster responded with HTTP %d", response.StatusCode)
	}

	r := make([]shardInfo, 0)
	if err = json.NewDecoder(response.Body).Decode(&r); err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}

	return r, nil
}

type nodeInfo struct {
	IP       string `json:"ip"`
	NodeRole string `json:"node.role"`
	Name     string `json:"name"`
}

func (s nodeInfo) IsMaster() bool {
	return s.NodeRole == "m"
}

func (s nodeInfo) IsDataNode() bool {
	return !s.IsMaster()
}

func (es *esClient) CatNodes() ([]nodeInfo, error) {
	url := fmt.Sprintf("http://%s/_cat/nodes?h=ip,node.role,name&format=json", es.endpoint)

	response, err := es.http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("request failed: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("cluster responded with HTTP %d", response.StatusCode)
	}

	r := make([]nodeInfo, 0)
	if err = json.NewDecoder(response.Body).Decode(&r); err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}

	return r, nil
}

type allocateEmptyPrimary struct {
	Index          string `json:"index"`
	Shard          int    `json:"shard"`
	Node           string `json:"node"`
	AcceptDataLoss bool   `json:"accept_data_loss"`
}

type rerouteRequest struct {
	Commands []map[string]interface{} `json:"commands"`
}

type rerouteResponse struct {
	Acknowledged bool `json:"acknowledged"`
}

func (es *esClient) RerouteShard(index string, shard int, node string) error {
	request := rerouteRequest{
		Commands: []map[string]interface{}{
			map[string]interface{}{
				"allocate_empty_primary": allocateEmptyPrimary{
					Index:          index,
					Shard:          shard,
					Node:           node,
					AcceptDataLoss: true,
				},
			},
		},
	}

	url := fmt.Sprintf("http://%s/_cluster/reroute", es.endpoint)
	body, _ := json.Marshal(request)

	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	response, err := es.http.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("cluster responded with HTTP %d", response.StatusCode)
	}

	r := rerouteResponse{}
	if err = json.NewDecoder(response.Body).Decode(&r); err != nil {
		return fmt.Errorf("failed to decode response: %v", err)
	}

	if !r.Acknowledged {
		return errors.New("request was not acknowledged")
	}

	return nil
}
