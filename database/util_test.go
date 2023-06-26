/*
*

	@author: Allen
	@since: 2023/3/8
	@desc: //TODO

*
*/
package database

import (
	"github.com/Allen9012/Godis/datastruct/dict"
	"github.com/Allen9012/Godis/datastruct/lock"
)

func makeTestDB() *DB {
	return &DB{
		data: dict.MakeSyncDict(),
		//修改一个bug，增加一个空的实现
		addAof: func(line CmdLine) {},
		// 初始化map 赋值一个SyncMap
		ttlMap: dict.MakeSyncDict(),
		locker: lock.Make(lockerSize),
	}
}
