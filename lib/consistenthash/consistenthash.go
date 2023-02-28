/**
  @author: Allen
  @since: 2023/2/28
  @desc: //一致性Hash
**/
package consistenthash

import (
	"hash/crc32"
	"sort"
)

type HashFunc func(data []byte) uint32

type NodeMap struct {
	hashFunc    HashFunc
	nodeHashs   []int          //需要排序由于sort支持的是int，64机器是支持的
	nodehashMap map[int]string // 记录的是节点的Hash值和节点的映射
}

// NewNodeMap creates a new NodeMap
func NewNodeMap(fn HashFunc) *NodeMap {
	m := &NodeMap{
		hashFunc:    fn,
		nodehashMap: make(map[int]string),
	}
	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

// IsEmpty returns if there is no node in NodeMap
func (m *NodeMap) IsEmpty() bool {
	return len(m.nodeHashs) == 0
}

// AddNode add the given nodes into consistent hash circle
func (m *NodeMap) AddNode(keys ...string) {
	// 1. 获取节点的hash
	// 2. 加入节点的切片
	// 3. 排序
	for _, key := range keys {
		// 判断是不是空串
		if key == "" {
			continue
		}
		hash := int(m.hashFunc([]byte(key)))
		m.nodeHashs = append(m.nodeHashs, hash)
		m.nodehashMap[hash] = key
	}
	sort.Ints(m.nodeHashs)
}

// PickNode gets the closest item in the hash to the provided key.
func (m *NodeMap) PickNode(key string) string {
	// 1.判断是否有节点
	// 2.获取这个节点的Hash
	// 3.查找对应的位置
	if m.IsEmpty() {
		return ""
	}
	hash := int(m.hashFunc([]byte(key)))
	index := sort.Search(len(m.nodeHashs), func(i int) bool {
		return m.nodeHashs[i] >= hash
	})
	if index == len(m.nodeHashs) {
		index = 0
	}
	return m.nodehashMap[m.nodeHashs[index]]
}
