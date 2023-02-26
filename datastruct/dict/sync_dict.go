/**
  @author: Allen
  @since: 2023/2/25
  @desc: // 系统最底层的保存数据的接口
**/
package dict

import "sync"

// SyncDict wraps a map, it is not thread safe
type SyncDict struct {
	m sync.Map //可以替换成更好的数据结构
}

// MakeSyncDict makes a new map
func MakeSyncDict() *SyncDict {
	return &SyncDict{}
}

// Get returns the binding value and whether the key is exist
func (dict *SyncDict) Get(key string) (val interface{}, exists bool) {
	value, ok := dict.m.Load(key)
	return value, ok
}

// Len returns the number of dict
func (dict *SyncDict) Len() int {
	length := 0
	dict.m.Range(func(key, val interface{}) bool {
		length++
		return true
	})
	return length
}

// Put puts key value into dict and returns the number of new inserted key-value
func (dict *SyncDict) Put(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key)
	dict.m.Store(key, val)
	if existed {
		// 表示Put成功但没有变化
		return 0
	}
	return 1
}

// PutIfAbsent puts value if the key is not exists and returns the number of updated key-value
func (dict *SyncDict) PutIfAbsent(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key)
	if existed {
		return 0
	}
	dict.m.Store(key, val)
	return 1
}

// PutIfExists puts value if the key is exist and returns the number of inserted key-value
func (dict *SyncDict) PutIfExists(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key)
	if existed {
		dict.m.Store(key, val)
		return 1
	}
	return 0
}

// Remove removes the key and return the number of deleted key-value
func (dict *SyncDict) Remove(key string) (result int) {
	_, existed := dict.m.Load(key)
	dict.m.Delete(key)
	if existed {
		return 1
	}
	return 0
}

// ForEach traversal the dict
func (dict *SyncDict) ForEach(consumer Consumer) {
	dict.m.Range(func(key, value any) bool {
		consumer(key.(string), value)
		return true
	})
}

// Keys returns all keys in dict
func (dict *SyncDict) Keys() []string {
	ret := make([]string, dict.Len())
	i := 0
	// 连续遍历
	dict.m.Range(func(key, value any) bool {
		ret[i] = key.(string)
		i++
		return true
	})
	return ret
}

// RandomKeys randomly returns keys of the given number, may contain duplicated key
func (dict *SyncDict) RandomKeys(limit int) []string {
	ret := make([]string, dict.Len())
	// 随机不连续遍历
	for i := 0; i < limit; i++ {
		dict.m.Range(func(key, value any) bool {
			ret[i] = key.(string)
			return false
		})
	}
	return ret
}

// RandomDistinctKeys randomly returns keys of the given number, won't contain duplicated key
func (dict *SyncDict) RandomDistinctKeys(limit int) []string {
	ret := make([]string, dict.Len())
	i := 0
	dict.m.Range(func(key, value any) bool {
		ret[i] = key.(string)
		i++
		if i == limit {
			return false
		}
		return true
	})
	return ret
}

// Clear removes all keys in dict
func (dict *SyncDict) Clear() {
	// 旧的让系统gc
	*dict = *MakeSyncDict()
}
