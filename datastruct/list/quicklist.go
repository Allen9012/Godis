/**
  @author: Allen
  @since: 2023/3/12
  @desc: //quicklist
**/
package list

import "container/list"

// pageSize must be even
const pageSize = 1024

// QuickList is a linked list of page (which type is []interface{})
// QuickList has better performance than LinkedList of Add, Range and memory usage
type QuickList struct {
	data *list.List // list of []interface{}
	size int
}

// iterator of QuickList, move between [-1, ql.Len()]
type iterator struct {
	node   *list.Element
	offset int
	ql     *QuickList
}

func NewQuickList() *QuickList {
	l := &QuickList{
		data: list.New(),
	}
	return l
}

/*--- iterator ---*/

//
// get
//  @Description: 拿到value
//  @receiver iter
//  @return interface{}
//
func (iter *iterator) get() interface{} {
	return iter.page()[iter.offset]
}

//
// page
//  @Description: 拿到page
//  @receiver iter
//  @return []interface{}
//
func (iter *iterator) page() []interface{} {
	return iter.node.Value.([]interface{})
}

// next returns whether iter is in bound
func (iter *iterator) next() bool {
	page := iter.page()
	// 当前页偏移++
	if iter.offset < len(page)-1 {
		iter.offset++
		return true
	}
	// move to next page
	if iter.node == iter.ql.data.Back() {
		// already at last node
		iter.offset = len(page)
		return false
	}
	// page满了，下一个list的node
	iter.offset = 0
	iter.node = iter.node.Next()
	return true
}

// prev returns whether iter is in bound
func (iter *iterator) prev() bool {
	if iter.offset > 0 {
		iter.offset--
		return true
	}
	if iter.node == iter.ql.data.Front() {
		// already at last node
		iter.offset = -1
		return false
	}
	//前一页的最后一个偏移
	iter.node = iter.node.Prev()
	prevPage := iter.node.Value.([]interface{})
	iter.offset = len(prevPage) - 1
	return true
}

func (iter *iterator) atEnd() bool {
	if iter.ql.data.Len() == 0 {
		return true
	}
	if iter.node != iter.ql.data.Back() {
		return false
	}
	page := iter.page()
	return iter.offset == len(page)
}

func (iter *iterator) atBegin() bool {
	if iter.ql.data.Len() == 0 {
		return true
	}
	if iter.node != iter.ql.data.Front() {
		return false
	}
	return iter.offset == -1
}

func (iter *iterator) set(val interface{}) {
	page := iter.page()
	page[iter.offset] = val
}

/*--- QuickList ---*/

// Add adds value to the tail
func (ql *QuickList) Add(val interface{}) {
	ql.size++
	// 判断是否没有页
	// 判断最后一个页是否已满需要额外开
	if ql.data.Len() == 0 { //empty
		page := make([]interface{}, 0, pageSize)
		page = append(page, val)
		ql.data.PushBack(page)
		return
	}
	// assert list.data.Back() != nil
	backNode := ql.data.Back()
	backPage := backNode.Value.([]interface{})
	if len(backPage) == cap(backPage) { // full page, create new page
		page := make([]interface{}, 0, pageSize)
		page = append(page, val)
		ql.data.PushBack(page)
		return
	}
	// 直接插入
	backPage = append(backPage, val)
	backNode.Value = backPage
}

// find returns page and in-page-offset of given index
func (ql *QuickList) find(index int) *iterator {
	if ql == nil {
		panic("list is nil")
	}
	if index < 0 || index >= ql.size {
		panic("index out of bound")
	}
	var n *list.Element
	var page []interface{}
	var pageBegin int
	// 找到对应index的page,分前一半和过后一半查找
	if index < ql.size/2 {
		// search from front
		n = ql.data.Front()
		pageBegin = 0
		for {
			// assert: n != nil
			page = n.Value.([]interface{})
			if pageBegin+len(page) > index {
				break
			}
			pageBegin += len(page)
			n = n.Next()
		}
	} else {
		// search from back
		n = ql.data.Back()
		pageBegin = ql.size
		for {
			page = n.Value.([]interface{})
			pageBegin -= len(page)
			if pageBegin <= index {
				break
			}
			n = n.Prev()
		}
	}
	pageOffset := index - pageBegin
	return &iterator{
		node:   n,
		offset: pageOffset,
		ql:     ql,
	}
}

func (iter *iterator) remove() interface{} {
	// 1. 覆盖data
	// 2. 如果page不空修改offset
	// 3. 空page就修改node内容和offset
	page := iter.page()
	val := page[iter.offset]
	page = append(page[:iter.offset], page[iter.offset+1:]...)
	if len(page) > 0 {
		// page is not empty, update iter.offset only
		iter.node.Value = page
		if iter.offset == len(page) {
			// removed page[-1], node should move to next page
			if iter.node != iter.ql.data.Back() {
				iter.node = iter.node.Next()
				iter.offset = 0
			}
			// else: assert iter.atEnd() == true
		}
	} else {
		// page is empty, update iter.node and iter.offset
		if iter.node == iter.ql.data.Back() {
			// removed last element, ql is empty now
			iter.ql.data.Remove(iter.node)
			iter.node = nil
			iter.offset = 0
		} else {
			nextNode := iter.node.Next()
			iter.ql.data.Remove(iter.node)
			iter.node = nextNode
			iter.offset = 0
		}
	}
	iter.ql.size--
	return val
}

/*--- 封装iterator的方法入QuickList ---*/

// Get returns value at the given index
func (ql *QuickList) Get(index int) (val interface{}) {
	iter := ql.find(index)
	return iter.get()
}

// Set updates value at the given index, the index should between [0, list.size]
func (ql *QuickList) Set(index int, val interface{}) {
	iter := ql.find(index)
	iter.set(val)
}

//
// Insert
//  @Description: 在QuickList中插入到任意位置
//  @receiver ql
//  @param index
//  @param val
//
func (ql *QuickList) Insert(index int, val interface{}) {
	// 1. 如果插入在末尾就直接Add
	// 2. 找到index迭代器的位置，拿到下标页page
	// 3. 插入对应位置，如果没有满直接插入
	// 4. 否则需要开新的一页，需要这一页分裂成两页
	// 5. 看offset判断分在前一页还是后一页
	// 6. 最后把这一页插入
	if index == ql.size { // insert at
		ql.Add(val)
		return
	}
	iter := ql.find(index)
	page := iter.node.Value.([]interface{})
	if len(page) < pageSize { // insert into not full page
		// 这行的意思是0-offset下标拼接offset-size，表示在offset之前一个位置插入数据和
		page := append(page[:iter.offset+1], page[iter.offset:]...)
		page[iter.offset] = val
		iter.node.Value = page
		ql.size++
		return
	}
	// insert into a full page may cause memory copy, so we split a full page into two half pages
	var nextPage []interface{}
	nextPage = append(nextPage, page[pageSize/2:]...) // pageSize must be even
	page = page[:pageSize/2]
	if iter.offset < len(page) {
		page = append(page[:iter.offset+1], page[iter.offset:]...)
		page[iter.offset] = val
	} else {
		i := iter.offset - pageSize/2
		nextPage = append(nextPage[:i+1], nextPage[i:]...)
		nextPage[i] = val
	}
	// store current page and next page
	iter.node.Value = page
	ql.data.InsertAfter(nextPage, iter.node)
	ql.size++
}

// Remove removes value at the given index
func (ql *QuickList) Remove(index int) interface{} {
	iter := ql.find(index)
	return iter.remove()
}

// Len returns the number of elements in list
func (ql *QuickList) Len() int {
	return ql.size
}

// RemoveLast removes the last element and returns its value
func (ql *QuickList) RemoveLast() interface{} {
	// 1. 判断是否List有值
	// 2. 如果是Page最后一个节点需要删除页
	// 3. 否则获取这一页的最后一个元素删除
	if ql.Len() == 0 {
		return nil
	}
	ql.size--
	// List 拿到 Page
	lastNode := ql.data.Back()
	lastPage := lastNode.Value.([]interface{})
	if len(lastPage) == 1 {
		ql.data.Remove(lastNode)
		return lastPage[0]
	}
	val := lastPage[len(lastPage)-1]
	lastPage = lastPage[:len(lastPage)-1]
	lastNode.Value = lastPage
	return val
}

//
// RemoveAllByVal removes all elements with the given val
//  @Description:
//  @receiver ql
//  @param expected
//  @return int
//	1. 获取第一个节点
//  2. 遍历找到就++，然后删除
func (ql *QuickList) RemoveAllByVal(expected Expected) int {
	if ql.size == 0 {
		return 0
	}
	iter := ql.find(0)
	removed := 0
	for !iter.atEnd() {
		if expected(iter.get()) {
			iter.remove()
			removed++
		} else {
			iter.next()
		}
	}
	return removed
}

//
// RemoveByVal removes at most `count` values of the specified value in this list
// scan from left to right
//  @Description:
//  @receiver ql
//  @param expected
//  @param count
//  @return int
//
func (ql *QuickList) RemoveByVal(expected Expected, count int) int {
	if ql.size == 0 {
		return 0
	}
	iter := ql.find(0)
	removed := 0
	for !iter.atEnd() {
		if expected(iter.get()) {
			iter.remove()
			removed++
			if removed == count {
				break
			}
		} else {
			iter.next()
		}
	}
	return removed
}

// ReverseRemoveByVal
// 反向移除node at most `count` values of the specified value in this list
// scan from left to right
// 获取最后一个节点的迭代器
func (ql *QuickList) ReverseRemoveByVal(expected Expected, count int) int {
	if ql.size == 0 {
		return 0
	}
	iter := ql.find(ql.size - 1)
	removed := 0
	for !iter.atBegin() {
		if expected(iter.get()) {
			iter.remove()
			removed++
			if removed == count {
				break
			}
		}
		iter.prev()
	}
	return removed
}

// ForEach visits each element in the list
// if the consumer returns false, the loop will be break
func (ql *QuickList) ForEach(consumer Consumer) {
	if ql == nil {
		panic("list is nil")
	}
	if ql.Len() == 0 {
		return
	}
	// 从头开始遍历
	iter := ql.find(0)
	i := 0
	for {
		goNext := consumer(i, iter.get())
		if !goNext {
			break
		}
		i++
		if !iter.next() {
			break
		}
	}
}

func (ql *QuickList) Contains(expected Expected) bool {
	contains := false
	ql.ForEach(func(i int, actual interface{}) bool {
		if expected(actual) {
			contains = true
			return false
		}
		return true
	})
	return contains
}

// Range returns elements which index within [start, stop)
func (ql *QuickList) Range(start int, stop int) []interface{} {
	if start < 0 || start >= ql.Len() {
		panic("`start` out of range")
	}
	if stop < start || stop > ql.Len() {
		panic("`stop` out of range")
	}
	sliceSize := stop - start
	ret := make([]interface{}, 0, sliceSize)
	iter := ql.find(start)
	i := 0
	for i < sliceSize {
		ret = append(ret, iter.get())
		iter.next()
		i++
	}
	return ret
}
