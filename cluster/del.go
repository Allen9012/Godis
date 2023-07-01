/*
*

	@author: Allen
	@since: 2023/2/28
	@desc: //需要单独写del命令

*
*/
package cluster

import (
	"github.com/Allen9012/Godis/godis/reply"
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
	var errReply reply.ErrorReply
	var deleted int64 = 0
	for _, v := range replies {
		if reply.IsErrorReply(v) {
			errReply = v.(reply.ErrorReply)
			break
		}
		intReply, ok := v.(*reply.IntReply)
		if !ok {
			errReply = reply.MakeErrReply("error")
		}
		deleted += intReply.Code
	}

	if errReply == nil {
		return reply.MakeIntReply(deleted)
	}
	return reply.MakeErrReply("error occurs: " + errReply.Error())
}
