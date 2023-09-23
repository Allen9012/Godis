package idgenerator

import (
	"testing"
)

/**
  Copyright © 2023 github.com/Allen9012 All rights reserved.
  @author: Allen
  @since: 2023/9/23
  @desc:
  @modified by:
**/

func TestMGenerator(t *testing.T) {
	gen := MakeGenerator("a")
	ids := make(map[int64]struct{})
	size := int(1e6)
	for i := 0; i < size; i++ {
		id := gen.NextID()
		_, ok := ids[id]
		if ok {
			t.Errorf("duplicated id: %d, time: %d, seq: %d", id, gen.lastStamp, gen.sequence)
		}
		ids[id] = struct{}{}
	}
}
