package sortedset

import (
	"math/bits"
	"math/rand"
)

const (
	// 限制最大层数
	maxLevel = 16
)

// Element is a key-score pair
type Element struct {
	Member string
	Score  float64
}

// Level aspect of a node
type Level struct {
	next *node // 指向同层中的下一个节点
	span int64 // 到 next 跳过的节点数
}

// 一个跳表的节点
type node struct {
	Element          // 元素的名称和score值
	prev    *node    // 后向的指针
	level   []*Level // 表示跳表层数
}

// 整个跳表结构体
type skiplist struct {
	header *node // 头节点
	tail   *node // 尾节点
	length int64 // 跳表长度
	level  int16 // 跳表层数(当前最大层数)
}

func make_node(level int16, score float64, member string) *node {
	n := &node{
		Element: Element{
			Member: member,
			Score:  score,
		},
		level: make([]*Level, level),
	}
	for i := range n.level {
		n.level[i] = new(Level)
	}
	return n
}

func make_skiplist() *skiplist {
	return &skiplist{
		header: make_node(maxLevel, 0, ""),
		level:  1,
	}
}

// 随机获得一层
func random_level() int16 {
	total := uint64(1)<<uint64(maxLevel) - 1
	k := rand.Uint64() % total
	return maxLevel - int16(bits.Len64(k+1)) + 1
}

/* -----	RETRIEVE	-----*/
//
// getRank
//  @Description: 1 based rank, 0 means member not found
//  @receiver skiplist
//  @param member
//  @param score
//  @return int64
//
func (skiplist *skiplist) get_rank(member string, score float64) int64 {
	var rank int64 = 0
	x := skiplist.header
	for i := skiplist.level - 1; i >= 0; i-- {
		for x.level[i].next != nil &&
			(x.level[i].next.Score < score ||
				(x.level[i].next.Score == score &&
					x.level[i].next.Member <= member)) {
			rank += x.level[i].span
			x = x.level[i].next
		}

		/* x might be equal to zsl->header, so test if obj is non-NULL */
		if x.Member == member {
			return rank
		}
	}
	return 0
}

// get_by_rank
//
//	@Description: 1 based rank, 0 means member not found
//	@receiver skiplist
//	@param rank
func (skiplist *skiplist) get_by_rank(rank int64) *node {
	var i int64 = 0
	n := skiplist.header
	// scan from top level
	// 若当前层的下一个节点已经超过目标 (i+n.level[level].span > rank)，则结束当前层搜索进入下一层
	for lv := skiplist.level - 1; lv >= 0; lv-- {
		for n.level[lv].next != nil && (i+n.level[lv].span) <= rank {
			i += n.level[lv].span
			n = n.level[lv].next
		}
		if i == rank {
			return n
		}
	}
	return nil
}

////
//// get_rank
////  @Description: 1 based rank, 0 means member not found
////  @receiver skiplist
////  @param member
////  @param score
////  @return int64
////
//func (skiplist *skiplist) get_rank(member string, score float64) int64 {
//
//}

// has_in_range
//
//	@Description check if there is any element in range (min, max)
//	@receiver skiplist
//	@param min
//	@param max
//	@return bool
func (skiplist *skiplist) has_in_range(min Border, max Border) bool {
	if min.isIntersected(max) { //是有交集的，则返回false
		return false
	}
	//	min > tail
	n := skiplist.tail
	// min must larger than
	if n == nil || !min.less(&n.Element) {
		return false
	}
	// max < head
	n = skiplist.header.level[0].next
	if n == nil || !max.greater(&n.Element) {
		return false
	}
	return true
}

// getFirstInRange
//
//	@Description:找到分数范围内第一个节点
//	@receiver skiplist
//	@param min
//	@param max
//	@return *node
func (skiplist *skiplist) get_first_in_range(min Border, max Border) *node {
	if !skiplist.has_in_range(min, max) {
		return nil
	}
	n := skiplist.header
	// scan from top level
	for lv := skiplist.level - 1; lv >= 0; lv-- {
		// 如果还有下一个节点，且下一个节点的分数小于等于min，则继续向后遍历
		for n.level[lv].next != nil && !min.less(&n.level[lv].next.Element) {
			n = n.level[lv].next
		}
	}
	/* This is an inner range, so the next node cannot be NULL. */
	n = n.level[0].next
	if !max.greater(&n.Element) {
		return nil
	}
	return n
}

// getLastInRange
//
//	@Description: 找到分数范围内最后一个节点
//	@receiver skiplist
//	@param min
//	@param max
//	@return *node
func (skiplist *skiplist) get_last_in_range(min Border, max Border) *node {
	if !skiplist.has_in_range(min, max) {
		return nil
	}
	n := skiplist.header
	// scan from top level
	for lv := skiplist.level - 1; lv >= 0; lv-- {
		// 如果还有下一个节点，且下一个节点的分数小于等于max，则继续向后遍历
		for n.level[lv].next != nil && max.greater(&n.level[lv].next.Element) {
			n = n.level[lv].next
		}
	}
	if !min.less(&n.Element) {
		return nil
	}
	return n
}

/*	-----	CREATE	 -----  */
//
// insert
//  @Description: insert a new node
//  @receiver skiplist
//  @param member
//  @param score
//  @return *node
//
func (skiplist *skiplist) insert(member string, score float64) *node {
	// 先找到节点，然后按照单链表的方式插
	update := make([]*node, maxLevel) // link new node with node in `update` 记录每一层需要修改的节点
	rank := make([]int64, maxLevel)   // 保存每个节点的排名，用于计算span

	node := skiplist.header
	// find position to insert
	for i := skiplist.level - 1; i >= 0; i-- {
		// 处理rank
		if i == skiplist.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1] // store rank that is crossed to reach the insert position
		}
		if node.level[i] != nil {
			// traverse the skiplist
			for node.level[i].next != nil &&
				// 目标分数小，或者same score, different key
				(node.level[i].next.Score < score || node.level[i].next.Score == score && node.level[i].next.Member < member) {
				rank[i] += node.level[i].span
				node = node.level[i].next
			}
		}
		update[i] = node
	}

	level := random_level()
	//	 extend skiplist level
	if level > skiplist.level {
		for i := skiplist.level; i < level; i++ {
			rank[i] = 0
			update[i] = skiplist.header
			update[i].level[i].span = skiplist.length
		}
		skiplist.level = level
	}
	// make node and link into skiplist
	node = make_node(level, score, member)
	for i := int16(0); i < level; i++ {
		// cur->next
		node.level[i].next = update[i].level[i].next
		// prev->cur
		update[i].level[i].next = node
		// update span covered by update[i] as node is inserted here
		node.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}
	// increment span for untouched levels
	for i := level; i < skiplist.level; i++ {
		update[i].level[i].span++
	}
	//	set prev node (only first level)
	if update[0] == skiplist.header {
		node.prev = nil
	} else {
		node.prev = update[0]
	}
	if node.level[0].next != nil {
		node.level[0].next.prev = node
	} else {
		skiplist.tail = node
	}
	skiplist.length++
	return node
}

/*	-----	DELETE   -----  */

// remove
//
//	@Description: has found and removed node
//	@receiver skiplist
//	@param member
//	@param score
//	@return bool
func (skiplist *skiplist) remove(member string, score float64) bool {
	/*
	 * find backward node (of target) or last node of each level
	 * their forward need to be updated
	 */
	update := make([]*node, maxLevel)
	node := skiplist.header
	for i := skiplist.level - 1; i >= 0; i-- {
		for node.level[i].next != nil &&
			(node.level[i].next.Score < score || node.level[i].next.Score == score && node.level[i].next.Member < member) {
			node = node.level[i].next
		}
		update[i] = node
	}
	// 找到下一个节点，移除中间
	node = node.level[0].next
	if node != nil && score == node.Score && node.Member == member {
		skiplist.remove_node(node, update)
		return true
	}
	return false
}

// Remove_node
//
//	@Description: remove node from skiplist
//	@receiver skiplist
//	@param node
//	@param update
func (skiplist *skiplist) remove_node(node *node, update []*node) {
	// 修改span数量和指针的连接
	for i := int16(0); i < skiplist.level; i++ {
		// 如果下一个就是要删除的节点
		if update[i].level[i].next == node {
			update[i].level[i].span += node.level[i].span - 1
			update[i].level[i].next = node.level[i].next
		} else {
			// 如果下一个不是要删除的节点
			update[i].level[i].span--
		}
	}
	// 修改底层节点的prev指针
	if node.level[0].next != nil {
		node.level[0].next.prev = node.prev
	} else {
		skiplist.tail = node.prev
	}
	// 如果要删除的是最高层的节点，且最高层已经为空，需要修改skiplist的level
	for skiplist.level > 1 && skiplist.header.level[skiplist.level-1].next == nil {
		skiplist.level--
	}
	skiplist.length--
}

// remove_range
//
//	@Description: return removed elements
//	@receiver skiplist
//	@param min
//	@param max
//	@param limit
//	@return removed
func (skiplist *skiplist) remove_range(min Border, max Border, limit int) (removed []*Element) {
	update := make([]*node, maxLevel)
	removed = make([]*Element, 0)
	// find backward nodes (of target range) or last node of each level
	node := skiplist.header
	for i := skiplist.level - 1; i >= 0; i-- {
		for node.level[i].next != nil {
			if min.less(&node.level[i].next.Element) {
				break
			}
			node = node.level[i].next
		}
		update[i] = node
	}
	// node is the first one within range
	node = node.level[0].next

	// remove nodes in range
	for node != nil {
		if !max.greater(&node.Element) { // already out of range
			break
		}
		next := node.level[0].next
		// 需要返回的内容
		removedElement := node.Element
		removed = append(removed, &removedElement)
		skiplist.remove_node(node, update)
		if limit > 0 && len(removed) == limit {
			break
		}
		node = next
	}
	return removed
}

// RemoveRangeByRank
//
//	@Description: 通过排名的start和stop来移除元素
//	@receiver skiplist
//	@param start
//	@param stop
//	@return removed
func (skiplist *skiplist) remove_range_by_rank(start int64, stop int64) (removed []*Element) {
	var i int64 = 0 // rank of iterator
	update := make([]*node, maxLevel)
	removed = make([]*Element, 0)

	// scan from top level
	node := skiplist.header
	for level := skiplist.level - 1; level >= 0; level-- {
		// 可以一次跳过多个节点，快速找到对应的节点
		for node.level[level].next != nil && (i+node.level[level].span) < start {
			i += node.level[level].span
			node = node.level[level].next
		}
		update[level] = node
	}
	// 符合要求的第一个节点
	i++
	node = node.level[0].next // first node in range
	// remove nodes in range
	for node != nil && i < stop {
		next := node.level[0].next
		removedElement := node.Element
		removed = append(removed, &removedElement)
		skiplist.remove_node(node, update)
		node = next
		i++
	}
	return removed
}

/* -----	UPDATE	  -----  */
// no need to update
