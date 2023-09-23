package cluster

import (
	"github.com/Allen9012/Godis/interface/godis"
)

/**
  Copyright © 2023 github.com/Allen9012 All rights reserved.
  @author: Allen
  @since: 2023/9/23
  @desc:
  @modified by:
**/

var addresses = []string{"127.0.0.1:6399", "127.0.0.1:7379"}
var timeoutFlags = []bool{false, false}
var testCluster = mockClusterNodes(addresses, timeoutFlags)

type testClientFactory struct {
	nodes        []*Cluster
	timeoutFlags []bool
}

type testClient struct {
	targetNode  *Cluster
	timeoutFlag *bool
	conn        godis.Connection
}

// mockClusterNodes creates a fake cluster for test
// timeoutFlags should have the same length as addresses, set timeoutFlags[i] == true could simulate addresses[i] timeout
func mockClusterNodes(addresses []string, timeoutFlags []bool) []*Cluster {
	//nodes := make([]*Cluster, len(addresses))
	//// build fixedTopology
	//slots := make([]*Slot, slotCount)
	//nodeMap := make(map[string]*Node)
	//for _, addr := range addresses {
	//	nodeMap[addr] = &Node{
	//		ID:    addr,
	//		Addr:  addr,
	//		Slots: nil,
	//	}
	//}
	//for i := range slots {
	//	addr := addresses[i%len(addresses)]
	//	slots[i] = &Slot{
	//		ID:     uint32(i),
	//		NodeID: addr,
	//		Flags:  0,
	//	}
	//	nodeMap[addr].Slots = append(nodeMap[addr].Slots, slots[i])
	//}
	//factory := &testClientFactory{
	//	nodes:        nodes,
	//	timeoutFlags: timeoutFlags,
	//}
	//for i, addr := range addresses {
	//	topo := &fixedTopology{
	//		mu:         sync.RWMutex{},
	//		nodeMap:    nodeMap,
	//		slots:      slots,
	//		selfNodeID: addr,
	//	}
	//	nodes[i] = &Cluster{
	//		self:          addr,
	//		db:            database2.NewStandaloneServer(),
	//		transactions:  dict.MakeSimple(),
	//		idGenerator:   idgenerator.MakeGenerator(config.Properties.Self),
	//		topology:      topo,
	//		clientFactory: factory,
	//	}
	//}
	//return nodes

	// TODO Implement me
	panic("Implement me")
}
