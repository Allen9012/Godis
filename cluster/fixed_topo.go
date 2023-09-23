package cluster

import "sync"

/**
  Copyright © 2023 github.com/Allen9012 All rights reserved.
  @author: Allen
  @since: 2023/9/23
  @desc:
  @modified by:
**/

// fixedTopology is a fixed cluster topology, used for test
type fixedTopology struct {
	mu         sync.RWMutex
	nodeMap    map[string]*Node
	slots      []*Slot
	selfNodeID string
}
