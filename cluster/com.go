/*
*

	@author: Allen
	@since: 2023/2/28
	@desc: //节点之间的通信

*
*/
package cluster

import (
	"context"
	"errors"
	"github.com/Allen9012/Godis/interface/resp"
	"github.com/Allen9012/Godis/lib/utils"
	"github.com/Allen9012/Godis/redis/client"
	"github.com/Allen9012/Godis/redis/reply"
	"strconv"
)

// getPeerClient
//
//	 @Description: 获取一个peer连接池
//	 @receiver cluster
//	 @param peer
//	 @return *client.Client
//	 @return error
//		1. 判断是否有这个peer
//		2. 获取一个对象
//		3. 断言成真正的类型
func (cluster *ClusterDatabase) getPeerClient(peer string) (*client.Client, error) {
	pool, ok := cluster.peerconnection[peer]
	if !ok {
		return nil, errors.New("connection not found")
	}
	object, err := pool.BorrowObject(context.Background())
	if err != nil {
		return nil, err
	}
	c, ok := object.(*client.Client)
	if !ok {
		return nil, errors.New("wrong type")
	}
	return c, err
}

/* ---- 三种执行模式 ----- */

// returnPeerClient
//
//	@Description: 返回连接
//	@receiver cluster
//	@param peer
//	@param peerClient
//	@return error
func (cluster *ClusterDatabase) returnPeerClient(peer string, peerClient *client.Client) error {
	pool, ok := cluster.peerconnection[peer]
	if !ok {
		return errors.New("connection not found")
	}
	return pool.ReturnObject(context.Background(), peerClient)
}

// relay
// select db by c.GetDBIndex()
// cannot call Prepare, Commit, execRollback of self node
//
//	@Description: relays command to peer
//	@receiver cluster
//	@param peer
//	@param c
//	@param args
//	@return redis.Reply
func (cluster *ClusterDatabase) relay(peer string, c resp.Connection, args [][]byte) resp.Reply {
	if peer == cluster.self {
		return cluster.db.Exec(c, args)
	}
	peerClient, err := cluster.getPeerClient(peer)
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	defer func() {
		_ = cluster.returnPeerClient(peer, peerClient)
	}()
	peerClient.Send(utils.ToCmdLine("SELECT", strconv.Itoa(c.GetDBIndex())))
	return peerClient.Send(args)
}

// broadcast command to all node in cluster
//
//	@Description: 广播模式
//	@receiver cluster
//	@param c
//	@param args
//	@return map[string]redis.Reply
func (cluster *ClusterDatabase) broadcast(c resp.Connection, args [][]byte) map[string]resp.Reply {
	results := make(map[string]resp.Reply)
	for _, node := range cluster.nodes {
		result := cluster.relay(node, c, args)
		results[node] = result
	}
	return results
}
