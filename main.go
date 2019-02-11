package main

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
)

func main() {
	endpoint := "localhost:9200"

	if len(os.Args) > 1 {
		endpoint = os.Args[1]
	}

	log.Printf("Using Elasticsearch endpoint: %s", endpoint)

	es := NewElasticsearchClient(endpoint, nil)

	log.Printf("Listing available nodes...")
	nodes, err := es.CatNodes()
	if err != nil {
		log.Fatalf("Failed to list nodes: %v", err)
	}

	shardsPerNode := make(map[string][]shardInfo)

	for _, info := range nodes {
		if info.IsDataNode() {
			shardsPerNode[info.Name] = make([]shardInfo, 0)
		}
	}

	log.Printf("Listing shard allocations...")
	infos, err := es.CatShards()
	if err != nil {
		log.Fatalf("Failed to list shards: %v", err)
	}

	for _, info := range infos {
		if info.IsPrimary() {
			continue
		}

		slice := shardsPerNode[info.Node]
		slice = append(slice, info)
		shardsPerNode[info.Node] = slice
	}

	unassignedShards := shardsPerNode[""]
	delete(shardsPerNode, "")

	log.Printf("There are %d unassigned shards.", len(unassignedShards))

	for _, info := range unassignedShards {
		target := leastLoadedNode(shardsPerNode)

		log.Printf("Rerouting shard %s/%d => %s...", info.Index, info.ShardID(), target)

		err := es.RerouteShard(info.Index, info.ShardID(), target)
		if err != nil {
			log.Fatalf("Failed: %v", err)
		}

		slice := shardsPerNode[target]
		slice = append(slice, info)
		shardsPerNode[target] = slice
	}
}

func leastLoadedNode(m map[string][]shardInfo) string {
	counts := make([]string, 0)

	for node, infos := range m {
		counts = append(counts, fmt.Sprintf("%05d %s", len(infos), node))
	}

	sort.Strings(counts)

	if len(counts) == 0 {
		return ""
	}

	return strings.Split(counts[0], " ")[1]
}
