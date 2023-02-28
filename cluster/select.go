/**
  @author: Allen
  @since: 2023/2/28
  @desc: //select
**/
package cluster

import "Gedis/interface/resp"

func execSelect(cluster *ClusterDatabase, c resp.Connection, cmdAndArgs [][]byte) resp.Reply {
	return cluster.db.Exec(c, cmdAndArgs)
}
