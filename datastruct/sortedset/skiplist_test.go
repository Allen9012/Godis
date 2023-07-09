package sortedset

import (
	"math/bits"
	"math/rand"
	"testing"
)

/**
  Copyright © 2023 github.com/Allen9012 All rights reserved.
  @author: Allen
  @since: 2023/7/8
  @desc:
  @modified by:
**/

func Test_random_level(t *testing.T) {
	total := uint64(1)<<uint64(maxLevel) - 1
	t.Log(total)
	k := rand.Uint64() % total
	t.Log(k)
	ret := maxLevel - int16(bits.Len64(k+1)) + 1
	t.Log(int16(bits.Len64(k + 1)))
	t.Log(ret)
}
