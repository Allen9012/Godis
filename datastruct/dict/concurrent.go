package dict

import (
	"math"
	"sync"
)

// ConcurrentDict is thread safe map using sharding lock
type ConcurrentDict struct {
	table      []*shard // 分片
	count      int32    // 记录总数
	shardCount int      // 分片数量
}

type shard struct {
	m     map[string]interface{}
	mutex sync.Mutex
}

// 用于找到大于等于 param 的最小的 2 的幂次方的数。
func computeCapacity(param int) (size int) {
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	}
	return n + 1
}

// MakeConcurrentDict makes a new ConcurrentDict
func MakeConcurrentDict(shardCount int) *ConcurrentDict {
	// 初始化map
	shardCount = computeCapacity(shardCount)
	table := make([]*shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &shard{
			m: make(map[string]interface{}),
		}
	}
	d := &ConcurrentDict{
		shardCount: shardCount,
		count:      0,
		table:      table,
	}
	return d
}

func (dict *ConcurrentDict) Get(key string) (val interface{}, exists bool) {
	//TODO implement me
	panic("implement me")
}

func (dict *ConcurrentDict) Len() int {
	//TODO implement me
	panic("implement me")
}

func (dict *ConcurrentDict) Put(key string, val interface{}) (result int) {
	//TODO implement me
	panic("implement me")
}

func (dict *ConcurrentDict) PutIfAbsent(key string, val interface{}) (result int) {
	//TODO implement me
	panic("implement me")
}

func (dict *ConcurrentDict) PutIfExists(key string, val interface{}) (result int) {
	//TODO implement me
	panic("implement me")
}

func (dict *ConcurrentDict) Remove(key string) (result int) {
	//TODO implement me
	panic("implement me")
}

func (dict *ConcurrentDict) ForEach(consumer Consumer) {
	//TODO implement me
	panic("implement me")
}

func (dict *ConcurrentDict) Keys() []string {
	//TODO implement me
	panic("implement me")
}

func (dict *ConcurrentDict) RandomKeys(limit int) []string {
	//TODO implement me
	panic("implement me")
}

func (dict *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	//TODO implement me
	panic("implement me")
}

func (dict *ConcurrentDict) Clear() {
	//TODO implement me
	panic("implement me")
}
