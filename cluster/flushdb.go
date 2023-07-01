/*
*

	@author: Allen
	@since: 2023/2/28
	@desc: //flushdb 需要广播

*
*/
package cluster

import (
	"github.com/Allen9012/Godis/godis/reply"
	"github.com/Allen9012/Godis/interface/godis"
)

// FlushDB removes all data in current database
//
//	@Description:
//	@param cluster
//	@param c
//	@param args
//	@return redis.Reply
//
// 1. 广播所有节点
// 2. 遍历所有节点判断一下
// 3. 返回响应
func FlushDB(cluster *ClusterDatabase, c godis.Connection, args [][]byte) godis.Reply {
	replies := cluster.broadcast(c, args)
	var errReply reply.ErrorReply
	for _, v := range replies {
		if reply.IsErrorReply(v) {
			errReply = v.(reply.ErrorReply)
			break
		}
	}
	// 没有错误
	if errReply == nil {
		return &reply.OkReply{}
	}
	return reply.MakeErrReply("error occurs: " + errReply.Error())
}
