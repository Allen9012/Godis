/*
*

	@author: Allen
	@since: 2023/2/28
	@desc: //记录指令和实现模式之间的关系

*
*/
package cluster

import "github.com/Allen9012/Godis/interface/godis"

func makeRouter() map[string]CmdFunc {
	routerMap := make(map[string]CmdFunc)
	routerMap["exists"] = defaultFunc //exists k1
	routerMap["type"] = defaultFunc
	routerMap["rename"] = Rename
	routerMap["renamenx"] = Rename
	routerMap["set"] = defaultFunc
	routerMap["setnx"] = defaultFunc
	routerMap["get"] = defaultFunc
	routerMap["getset"] = defaultFunc
	routerMap["getnx"] = defaultFunc
	routerMap["ping"] = Ping
	routerMap["flushdb"] = FlushDB
	routerMap["del"] = Del
	routerMap["select"] = execSelect
	return routerMap
}

// GET Key // Set K1 v1
func defaultFunc(cluster *Cluster, c godis.Connection, cmdArgs [][]byte) godis.Reply {
	key := string(cmdArgs[0])
	peer := cluster.peerPicker.PickNode(key)
	return cluster.relay(peer, c, cmdArgs)
}
