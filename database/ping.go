/*
*

	@author: Allen
	@since: 2023/2/26
	@desc: //TODO

*
*/
package database

import (
	"github.com/Allen9012/Godis/interface/resp"
	"github.com/Allen9012/Godis/resp/reply"
)

// 初始化把所有的指令存储在cmdTable中
func init() {
	RegisterCommand("ping", Ping, 1)
}

func Ping(db *DB, args [][]byte) resp.Reply {
	return reply.MakePongReply()
}
