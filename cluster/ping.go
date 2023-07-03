/*
*

	@author: Allen
	@since: 2023/2/26
	@desc: //需要单独执行Ping命令

*
*/
package cluster

import "github.com/Allen9012/Godis/interface/godis"

func Ping(cluster *ClusterDatabase, c godis.Connection, cmdArgs [][]byte) godis.Reply {
	return cluster.db.Exec(c, cmdArgs)
}
