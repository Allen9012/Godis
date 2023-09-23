package cluster

/*
	@author: Allen
	@since: 2023/2/28
	@desc: //rename
*/

import (
	"github.com/Allen9012/Godis/godis/protocol"
	"github.com/Allen9012/Godis/interface/godis"
)

// Rename renames a key, the origin and the destination must within the same node
//
//	 @Description: 修改名字当前支持前后修改得出结果一样
//	 @param cluster
//	 @param c
//	 @param args
//	 @return redis.Reply
//		rename k1 k2
func Rename(cluster *Cluster, c godis.Connection, args [][]byte) godis.Reply {
	if len(args) != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'rename' command")
	}
	src := string(args[1])
	dest := string(args[2])
	// 获得节点string
	srcPeer := cluster.peerPicker.PickNode(src)
	destPeer := cluster.peerPicker.PickNode(dest)

	if srcPeer != destPeer {
		return protocol.MakeErrReply("ERR rename must within one slot in cluster mode")
	}
	return cluster.relay(srcPeer, c, args)
}
