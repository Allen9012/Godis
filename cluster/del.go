/*
*

	@author: Allen
	@since: 2023/2/28
	@desc: //需要单独写del命令

*
*/
package cluster

import (
	"github.com/Allen9012/Godis/godis/protocol"
	"github.com/Allen9012/Godis/interface/godis"
)

// Del atomically removes given writeKeys from cluster, writeKeys can be distributed on any node
// if the given writeKeys are distributed on different node, Del will use try-commit-catch to remove them
//
//	@Description:	del k1 k2 k3 k4 k5 依次删除可能需要删除多个key
//	@param cluster
//	@param c
//	@param args
//	@return redis.Reply
func Del(cluster *ClusterDatabase, c godis.Connection, args [][]byte) godis.Reply {
	replies := cluster.broadcast(c, args)
	var errReply protocol.ErrorReply
	var deleted int64 = 0
	for _, v := range replies {
		if protocol.IsErrorReply(v) {
			errReply = v.(protocol.ErrorReply)
			break
		}
		intReply, ok := v.(*protocol.IntReply)
		if !ok {
			errReply = protocol.MakeErrReply("error")
		}
		deleted += intReply.Code
	}

	if errReply == nil {
		return protocol.MakeIntReply(deleted)
	}
	return protocol.MakeErrReply("error occurs: " + errReply.Error())
}
