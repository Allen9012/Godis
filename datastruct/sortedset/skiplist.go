package sortedset

const (
	maxLevel = 16
)

// Element is a key-score pair
type Element struct {
	Member string
	Score  float64
}

// Level aspect of a node
type Level struct {
	forward *node // forward node has a greater score
	span    int64
}

type node struct {
	Element
	backward *node
	level    []*Level //level[0] is base level
}

type skipList struct {
	header *node
	tail   *node
	length int64
	level  int16
}
