package cluster

/*
*
	@author: Allen
	@since: 2023/2/28
	@desc: //select
*
*/
import "github.com/Allen9012/Godis/interface/godis"

func execSelect(cluster *ClusterDatabase, c godis.Connection, cmdAndArgs [][]byte) godis.Reply {
	return cluster.db.Exec(c, cmdAndArgs)
}
