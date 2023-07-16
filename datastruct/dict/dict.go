package dict

/**
  @author: Allen
  @since: 2023/2/25
  @desc: dict结构
**/

// Consumer 遍历的方法
type Consumer func(key string, val interface{}) bool

type Dict interface {
	Get(key string) (val interface{}, exists bool)
	Len() int // 有多少数据
	Put(key string, val interface{}) (result int)
	PutIfAbsent(key string, val interface{}) (result int)
	PutIfExists(key string, val interface{}) (result int)
	Remove(key string) (val interface{}, result int)
	ForEach(consumer Consumer)
	Keys() []string
	RandomKeys(limit int) []string         //返回limit数量的键
	RandomDistinctKeys(limit int) []string // 返回limit数量的不重复的键
	Clear()
}
