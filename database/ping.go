/*
*

	@author: Allen
	@since: 2023/2/26
	@desc: //TODO

*
*/
package database

import (
	"github.com/Allen9012/Godis/godis/protocol"
	"github.com/Allen9012/Godis/interface/godis"
)

// 初始化把所有的指令存储在cmdTable中
func init() {
	RegisterCommand("ping", Ping, 1)
}

func Ping(db *DB, args [][]byte) godis.Reply {
	return protocol.MakePongReply()
}
