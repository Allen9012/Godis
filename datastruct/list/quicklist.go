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
	return &QuickList{
		data: list.New(),
	}
}

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
