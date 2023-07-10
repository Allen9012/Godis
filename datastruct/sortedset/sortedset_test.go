package sortedset

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSortedSet_PopMin(t *testing.T) {
	var set = Make()
	set.Add("s1", 1)
	set.Add("s2", 2)
	set.Add("s3", 3)
	set.Add("s4", 4)
	assert.Equal(t, int64(4), set.Len())
	t.Log(set.skiplist.length)
	var results = set.PopMin(2)
	t.Log(results)
	assert.Equal(t, int64(2), set.Len())
	if results[0].Member != "s1" || results[1].Member != "s2" {
		t.Fail()
	}
}
