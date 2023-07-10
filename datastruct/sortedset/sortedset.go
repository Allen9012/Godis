package sortedset

import (
	"github.com/Allen9012/Godis/lib/logger"
	"strconv"
)

type SortedSet struct {
	dict     map[string]*Element
	skiplist *skiplist
}

// Make
//
//	@Description: Make a new SortedSet
//	注意dict不是一个线程安全的容器
//	@return *SortedSet
func Make() *SortedSet {
	return &SortedSet{
		dict:     make(map[string]*Element),
		skiplist: make_skiplist(),
	}
}

// Add puts member into set,  and returns whether it has inserted new node
func (sortedSet *SortedSet) Add(member string, score float64) bool {
	element, ok := sortedSet.dict[member]
	sortedSet.dict[member] = &Element{
		Member: member,
		Score:  score,
	}
	//如果原来有值，就执行更新操作
	if ok {
		if score != element.Score {
			sortedSet.skiplist.remove(member, element.Score)
			sortedSet.skiplist.insert(member, score)
		}
	}
	sortedSet.skiplist.insert(member, score)
	return true
}

// Len returns number of members in set
func (sortedSet *SortedSet) Len() int64 {
	return int64(len(sortedSet.dict))
}

// Get returns the given member
func (sortedSet *SortedSet) Get(member string) (element *Element, ok bool) {
	element, ok = sortedSet.dict[member]
	if !ok {
		return nil, false
	}
	return element, true
}

// Remove removes the given member from set
func (sortedSet *SortedSet) Remove(member string) bool {
	element, ok := sortedSet.dict[member]
	if ok {
		sortedSet.skiplist.remove(member, element.Score)
		delete(sortedSet.dict, member)
		return true
	}
	return false
}

// GetRank returns the rank of the given member, sort by ascending order, rank starts from 0
//
//	@Description:
//	@receiver sortedSet
//	@param member
//	@param desc 描述是否降序
//	@return rank
func (sortedSet *SortedSet) GetRank(member string, desc bool) (rank int64) {
	element, ok := sortedSet.dict[member]
	if !ok {
		return -1
	}
	r := sortedSet.skiplist.get_rank(member, element.Score)
	if desc {
		r = sortedSet.skiplist.length - r
	} else {
		r--
	}
	return r
}

// ForEachByRank visits each member which rank within [start, stop), sort by ascending order, rank starts from 0
func (sortedSet *SortedSet) ForEachByRank(start int64, stop int64, desc bool, consumer func(element *Element) bool) {
	size := int64(sortedSet.Len())
	// 检查参数合法性
	if start < 0 || start >= size {
		logger.Error("sortedset error: illegal start")
		panic(("illegal start") + strconv.FormatInt(start, 10))
	}
	if stop < 0 || stop > size {
		logger.Error("sortedset error: illegal stop")
		panic(("illegal stop") + strconv.FormatInt(stop, 10))
	}
	//	find first start
	var node *node
	if desc {
		node = sortedSet.skiplist.tail
		if start > 0 {
			node = sortedSet.skiplist.get_by_rank(size - start)
		}
	} else {
		// 头节点后的第一个节点
		node = sortedSet.skiplist.header.level[0].next
		if start > 0 {
			node = sortedSet.skiplist.get_by_rank(start + 1)
		}
	}
	// 组装返回值
	sliceSize := int(stop - start)
	for i := 0; i < sliceSize; i++ {
		if !consumer(&node.Element) {
			break
		}
		if desc {
			// 从后往前
			node = node.prev
		} else {
			node = node.level[0].next
		}
	}
}

// RangeByRank returns members which rank within [start, stop), sort by ascending order, rank starts from 0
func (sortedSet *SortedSet) RangeByRank(start int64, stop int64, desc bool) []*Element {
	sliceSize := int(stop - start)
	slice := make([]*Element, sliceSize)
	i := 0
	sortedSet.ForEachByRank(start, stop, desc, func(element *Element) bool {
		slice[i] = element
		i++
		return true
	})
	return slice
}

// RangeCount returns the number of  members which score or member within the given border
func (sortedSet *SortedSet) RangeCount(min Border, max Border) int64 {
	// 统计和返回次数
	var i int64 = 0
	//	ascending order
	sortedSet.ForEachByRank(0, sortedSet.Len(), false, func(element *Element) bool {
		gtMin := min.less(element) // greater than min
		if !gtMin {
			// has not into range, continue foreach
			return true
		}
		ltMax := max.greater(element) // less than max
		if !ltMax {
			// break through score border, break foreach
			return false
		}
		// gtMin && ltMax
		i++
		return true
	})
	return i
}

// RemoveRange removes members which score or member within the given border
func (sortedSet *SortedSet) RemoveRange(min Border, max Border) int64 {
	removed := sortedSet.skiplist.remove_range(min, max, 0)
	for _, element := range removed {
		delete(sortedSet.dict, element.Member)
	}
	return int64(len(removed))
}

// Range returns members which score or member within the given border
// param limit: <0 means no limit
func (sortedSet *SortedSet) Range(min Border, max Border, offset int64, limit int64, desc bool) []*Element {
	if limit == 0 || offset < 0 {
		return make([]*Element, 0)
	}
	slice := make([]*Element, 0)
	sortedSet.ForEach(min, max, offset, limit, desc, func(element *Element) bool {
		slice = append(slice, element)
		return true
	})
	return slice
}

func (sortedSet *SortedSet) PopMin(count int) []*Element {
	first := sortedSet.skiplist.get_first_in_range(scoreNegativeInfBorder, scorePositiveInfBorder)
	if first == nil {
		return nil
	}
	border := &ScoreBorder{
		Value:   first.Score,
		Exclude: false,
	}
	removed := sortedSet.skiplist.remove_range(border, scorePositiveInfBorder, count)
	for _, element := range removed {
		delete(sortedSet.dict, element.Member)
	}
	return removed
}

// ForEach visits members which score or member within the given border
func (sortedSet *SortedSet) ForEach(min Border, max Border, offset int64, limit int64, desc bool, consumer func(element *Element) bool) {
	// find start node
	var node *node
	if desc {
		node = sortedSet.skiplist.get_last_in_range(min, max)
	} else {
		node = sortedSet.skiplist.get_first_in_range(min, max)
	}
	// 走一个偏移
	for node != nil && offset > 0 {
		if desc {
			node = node.prev
		} else {
			node = node.level[0].next
		}
		offset--
	}
	// A negative limit returns all elements from the offset
	for i := 0; (i < int(limit) || limit < 0) && node != nil; i++ {
		if !consumer(&node.Element) {
			break
		}
		if desc {
			node = node.prev
		} else {
			node = node.level[0].next
		}
		if node == nil {
			break
		}
		gtMin := min.less(&node.Element) // greater than min
		ltMax := max.greater(&node.Element)
		if !gtMin || !ltMax {
			break // break through score border
		}
	}
}

// RemoveByRank removes member ranking within [start, stop)
// sort by ascending order and rank starts from 0
func (sortedSet *SortedSet) RemoveByRank(start int64, stop int64) int64 {
	removed := sortedSet.skiplist.remove_range_by_rank(start+1, stop+1)
	for _, element := range removed {
		delete(sortedSet.dict, element.Member)
	}
	return int64(len(removed))
}
