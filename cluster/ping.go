/**
  @author: Allen
  @since: 2023/2/26
  @desc: //TODO
**/
package cluster

import (
	"Gedis/interface/resp"
)

func Ping(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	return cluster.db.Exec(c, cmdArgs)
}
