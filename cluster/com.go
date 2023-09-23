package cluster

/*
	@author: Allen
	@since: 2023/2/28
	@desc: //节点之间的通信
*/
import (
	"context"
	"errors"
	"github.com/Allen9012/Godis/godis/client"
	"github.com/Allen9012/Godis/godis/protocol"
	"github.com/Allen9012/Godis/interface/godis"
	"github.com/Allen9012/Godis/lib/utils"
	"strconv"
)

type CmdLine = [][]byte

type clientFactory interface {
	GetPeerClient(peerAddr string) (peerClient, error)
	ReturnPeerClient(peerAddr string, peerClient peerClient) error
	NewStream(peerAddr string, cmdLine CmdLine) (peerStream, error)
	Close() error
}

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
func (cluster *Cluster) getPeerClient(peer string) (*client.Client, error) {
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
func (cluster *Cluster) returnPeerClient(peer string, peerClient *client.Client) error {
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
func (cluster *Cluster) relay(peer string, c godis.Connection, args [][]byte) godis.Reply {
	if peer == cluster.self {
		return cluster.db.Exec(c, args)
	}
	peerClient, err := cluster.getPeerClient(peer)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
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
func (cluster *Cluster) broadcast(c godis.Connection, args [][]byte) map[string]godis.Reply {
	results := make(map[string]godis.Reply)
	for _, node := range cluster.nodes {
		result := cluster.relay(node, c, args)
		results[node] = result
	}
	return results
}

// ensureKey will migrate key to current node if the key is in a slot migrating to current node
// invoker should provide with locks of key
func (cluster *Cluster) ensureKey(key string) protocol.ErrorReply {
	//slotId := getSlot(key)
	//cluster.slotMu.RLock()
	//slot := cluster.slots[slotId]
	//cluster.slotMu.RUnlock()
	//if slot == nil {
	//	return nil
	//}
	//if slot.state != slotStateImporting || slot.importedKeys.Has(key) {
	//	return nil
	//}
	//resp := cluster.relay(slot.oldNodeID, connection.NewFakeConn(), utils.ToCmdLine("DumpKey_", key))
	//if protocol.IsErrorReply(resp) {
	//	return resp.(protocol.ErrorReply)
	//}
	//if protocol.IsEmptyMultiBulkReply(resp) {
	//	slot.importedKeys.Add(key)
	//	return nil
	//}
	//dumpResp := resp.(*protocol.MultiBulkReply)
	//if len(dumpResp.Args) != 2 {
	//	return protocol.MakeErrReply("illegal dump key response")
	//}
	//// reuse copy to command ^_^
	//resp = cluster.db.Exec(connection.NewFakeConn(), [][]byte{
	//	[]byte("CopyTo"), []byte(key), dumpResp.Args[0], dumpResp.Args[1],
	//})
	//if protocol.IsErrorReply(resp) {
	//	return resp.(protocol.ErrorReply)
	//}
	//slot.importedKeys.Add(key)
	//return nil
	//TODO Implement me
	panic("need Implement")
}
