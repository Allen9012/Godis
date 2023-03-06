/**
  @author: Allen
  @since: 2023/2/26
  @desc: //需要单独执行Ping命令
**/
package cluster

import (
	"Gedis/interface/resp"
)

func Ping(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	return cluster.db.Exec(c, cmdArgs)
}
