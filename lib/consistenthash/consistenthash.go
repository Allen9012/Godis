package consistenthash

/**
  @author: Allen
  @since: 2023/2/28
  @desc: //一致性Hash
**/
import (
	"hash/crc32"
	"sort"
	"strconv"
	"strings"
)

// HashFunc defines function to generate hash code
type HashFunc func(data []byte) uint32

// NodeMap Map stores nodes so you can pick node from Map
type NodeMap struct {
	hashFunc HashFunc
	replicas int
	keys     []int          // sorted
	hashMap  map[int]string // 记录的是节点的Hash值和节点的映射
}

// NewNodeMap creates a new NodeMap
func NewNodeMap(replicas int, fn HashFunc) *NodeMap {
	m := &NodeMap{
		replicas: replicas,
		hashFunc: fn,
		hashMap:  make(map[int]string),
	}
	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

// IsEmpty returns if there is no node in NodeMap
func (m *NodeMap) IsEmpty() bool {
	return len(m.keys) == 0
}

// AddNode add the given nodes into consistent hash circle
func (m *NodeMap) AddNode(keys ...string) {
	// 1. 获取节点的hash
	// 2. 加入节点的切片
	// 3. 排序
	for _, key := range keys {
		if key == "" {
			continue
		}
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hashFunc([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}
	sort.Ints(m.keys)
}

// support hash tag
func getPartitionKey(key string) string {
	beg := strings.Index(key, "{")
	if beg == -1 {
		return key
	}
	end := strings.Index(key, "}")
	if end == -1 || end == beg+1 {
		return key
	}
	return key[beg+1 : end]
}

// PickNode gets the closest item in the hash to the provided key.
func (m *NodeMap) PickNode(key string) string {
	// 1.判断是否有节点
	// 2.获取这个节点的Hash
	// 3.查找对应的位置
	if m.IsEmpty() {
		return ""
	}
	// 支持根据 key 的 hashtag 来确定分布
	partitionKey := getPartitionKey(key)
	hash := int(m.hashFunc([]byte(partitionKey)))
	// sort.Search 会使用二分查找法搜索 keys 中满足 m.keys[i] >= hash 的最小 i 值
	index := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	// 若 key 的 hash 值大于最后一个虚拟节点的 hash 值，则 sort.Search 找不到目标
	// 这种情况下选择第一个虚拟节点
	if index == len(m.keys) {
		index = 0
	}
	// 将虚拟节点映射为实际地址
	return m.hashMap[m.keys[index]]
}
