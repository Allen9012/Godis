package sortedset

type SortedSet struct {
	dict     map[string]*Element
	skiplist *skiplist
}

// Make
//
//  @Description: Make a new SortedSet
//  注意dict不是一个线程安全的容器
//  @return *SortedSet
//
func Make() *SortedSet {
	return &SortedSet{
		dict:     make(map[string]*Element),
		skiplist: make_skiplist(),
	}
}

//// Add puts member into set,  and returns whether it has inserted new node
//func (sortedSet *SortedSet) Add(member string, score float64) bool {
//
//}
//
//// Len returns number of members in set
//func (sortedSet *SortedSet) Len() int64 {
//
//}
//
//// Get returns the given member
//func (sortedSet *SortedSet) Get(member string) (element *Element, ok bool) {
//
//}
//
//// Remove removes the given member from set
//func (sortedSet *SortedSet) Remove(member string) bool {
//
//}
//
//// GetRank returns the rank of the given member, sort by ascending order, rank starts from 0
//func (sortedSet *SortedSet) GetRank(member string, desc bool) (rank int64) {
//
//}
//
//// ForEachByRank visits each member which rank within [start, stop), sort by ascending order, rank starts from 0
//func (sortedSet *SortedSet) ForEachByRank(start int64, stop int64, desc bool, consumer func(element *Element) bool) {
//
//}
//
//// RangeByRank returns members which rank within [start, stop), sort by ascending order, rank starts from 0
//func (sortedSet *SortedSet) RangeByRank(start int64, stop int64, desc bool) []*Element {
//
//}
//
//// RangeCount returns the number of  members which score or member within the given border
//func (sortedSet *SortedSet) RangeCount(min Border, max Border) int64 {
//
//}
//
//// RemoveRange removes members which score or member within the given border
//func (sortedSet *SortedSet) RemoveRange(min Border, max Border) int64 {
//
//}
//
//// Range returns members which score or member within the given border
//// param limit: <0 means no limit
//func (sortedSet *SortedSet) Range(min Border, max Border, offset int64, limit int64, desc bool) []*Element {
//
//}
//
//func (sortedSet *SortedSet) PopMin(count int) []*Element {
//
//}
//
//// ForEach visits members which score or member within the given border
//func (sortedSet *SortedSet) ForEach(min Border, max Border, offset int64, limit int64, desc bool, consumer func(element *Element) bool) {
//
//}
//
//func (sortedSet *SortedSet) PopMin(count int) []*Element {
//
//}
//
//// RemoveByRank removes member ranking within [start, stop)
//// sort by ascending order and rank starts from 0
//func (sortedSet *SortedSet) RemoveByRank(start int64, stop int64) int64 {
//
//}
