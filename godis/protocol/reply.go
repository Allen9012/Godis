package protocol

import (
	"bytes"
	"github.com/Allen9012/Godis/interface/godis"
	"strconv"
)

// 动态回复

var (
	nullBulkReplyBytes = []byte("$-1")
	CRLF               = "\r\n"
)

/* ---- Bulk Reply ---- */

// BulkReply 单字符串
type BulkReply struct {
	Arg []byte
}

// ToBytes "$5\r\nallen\r\n"
func (r *BulkReply) ToBytes() []byte {
	if len(r.Arg) == 0 {
		return nullBulkReplyBytes
	}
	return []byte("$" + strconv.Itoa(len(r.Arg)) + CRLF + string(r.Arg) + CRLF)
}

func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{Arg: arg}
}

/* ---- Multi Bulk Reply ---- */

// MultiBulkReply 多个字符串
type MultiBulkReply struct {
	Args [][]byte
}

// ToBytes *3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$5\r\nHello\r\n
func (r *MultiBulkReply) ToBytes() []byte {
	argLen := len(r.Args)
	// 方便字符串拼装
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range r.Args {
		if arg == nil {
			buf.WriteString("$-1" + CRLF)
		} else {
			buf.WriteString("$" + strconv.Itoa(len(arg)) + CRLF + string(arg) + CRLF)
		}
	}
	return buf.Bytes()
}

func MakeMultiBulkReply(arg [][]byte) *MultiBulkReply {
	return &MultiBulkReply{Args: arg}
}

/* ---- Multi Raw Reply ---- */

// MultiRawReply store complex list structure, for example GeoPos command
type MultiRawReply struct {
	Replies []godis.Reply
}

// MakeMultiRawReply creates MultiRawReply
func MakeMultiRawReply(replies []godis.Reply) *MultiRawReply {
	return &MultiRawReply{
		Replies: replies,
	}
}

/* ---- Status Reply ---- */

// StatusReply stores a simple status string	+OK\r\n
type StatusReply struct {
	Status string
}

// MakeStatusReply creates StatusReply
func MakeStatusReply(status string) *StatusReply {
	return &StatusReply{
		Status: status,
	}
}

// ToBytes marshal redis.Reply
func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + CRLF)
}

// IsOKReply returns true if the given protocol is +OK
func IsOKReply(reply godis.Reply) bool {
	return string(reply.ToBytes()) == "+OK\r\n"
}

/* ---- Int Reply ---- */

// IntReply stores an int64 number :1000\r\n
type IntReply struct {
	Code int64
}

// MakeIntReply creates int reply
func MakeIntReply(code int64) *IntReply {
	return &IntReply{
		Code: code,
	}
}

// ToBytes marshal redis.Reply
func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}

/* ---- Error Reply ---- */

// ErrorReply is an error and redis.Reply
type ErrorReply interface {
	Error() string
	ToBytes() []byte
}

// StandardErrReply represents server error
type StandardErrReply struct {
	Status string
}

// ToBytes marshal redis.Reply 		-ERR unknown command 'foobar'\r\n
func (r *StandardErrReply) ToBytes() []byte {
	return []byte("-" + r.Status + CRLF)
}

func (r *StandardErrReply) Error() string {
	return r.Status
}

// MakeErrReply creates StandardErrReply
func MakeErrReply(status string) *StandardErrReply {
	return &StandardErrReply{
		Status: status,
	}
}

// IsErrorReply returns true if the given reply is error
func IsErrorReply(reply godis.Reply) bool {
	return reply.ToBytes()[0] == '-'
}
