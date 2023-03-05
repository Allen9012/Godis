/**
  @author: Allen
  @since: 2023/3/5
  @desc: // 记录特别的aof
**/
package aof

import (
	"Gedis/resp/reply"
	"strconv"
	"time"
)

var pExpireAtBytes = []byte("PEXPIREAT") //设置过期时间的命令

// MakeExpireCmd generates command line to set expiration for the given key
func MakeExpireCmd(key string, expireAt time.Time) *reply.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = pExpireAtBytes
	args[1] = []byte(key)
	args[2] = []byte(strconv.FormatInt(expireAt.UnixNano()/1e6, 10))
	return reply.MakeMultiBulkReply(args)
}
