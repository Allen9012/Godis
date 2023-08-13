package consistenthash

import "testing"

/*
*

	Copyright © 2023 github.com/Allen9012 All rights reserved.
	@author: Allen
	@since: 2023/8/13
	@desc:
	@modified by:

*
*/
func TestHash(t *testing.T) {
	m := NewNodeMap(3, nil)
	m.AddNode("a", "b", "c", "d")
	if m.PickNode("zxc") != "a" {
		t.Error("wrong answer")
	}
	if m.PickNode("123{abc}") != "b" {
		t.Error("wrong answer")
	}
	if m.PickNode("abc") != "b" {
		t.Error("wrong answer")
	}
}
