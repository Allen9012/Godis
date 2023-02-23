package parser

import (
	"Gedis/interface/resp"
	"Gedis/resp/reply"
	"bufio"
	"errors"
	"io"
	"strconv"
	"strings"
)

// PayLoad 客户端发送的数据
type PayLoad struct {
	// 服务端和客户交互的内容实际是类似的，都是reply
	Data resp.Reply
	Err  error
}

// 解析单行或者多行数据
type readState struct {
	readingMultiLine  bool     // 是否要读多行数据
	expectedArgsCount int      // 参数数量
	msgType           byte     // 数据类型
	args              [][]byte // 具体的传送来的数据
	bulkLen           int64    // 具体的长度 $后面的数字
}

// 是否解析完成
func (s *readState) finished() bool {
	//特殊情况
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

// ParseStream 实际可以调用的支持并发的异步的Parser 返回一个只发送的管道
func ParseStream(reader io.Reader) <-chan *PayLoad {
	ch := make(chan *PayLoad)
	go parse0(reader, ch)
	return ch
}

// parse0 解析器核心
func parse0(reader io.Reader, ch chan<- *PayLoad) {

}

//
// readLine
//  @Description: 一次读取一行
//  @param bufreader
//  @param state
//  @return []byte	返回的类似$3\r\n
//  @return bool	是否有IO错误
//  @return error
//
func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {
	//特殊情况： *3\r\n$3\r\n$7\r\n[k\r\ney]\r\n$5\r\nvalue\r\n
	//1. \r\n切分行
	//2. 如果有$数字，表示严格读取 \r\n是数据本身不能分行
	var msg []byte
	var err error

	if state.bulkLen == 0 { //1. \r\n切分行
		//表示没有预设的个数
		msg, err = bufReader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
		return msg, false, nil
	} else { //2. 如果有$数字，表示严格读取 \r\n是数据本身不能分行
		msg := make([]byte, state.bulkLen+2) // 多两个\r\n
		_, err = io.ReadFull(bufReader, msg)
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
		state.bulkLen = 0 // 读下一行的时候更新buklen
	}
	return msg, false, err
}

//
// parseMultiBulkHeader
//  @Description: readLine之后具体取出响应内容
//  @param msg
//  @param state
//  @return error	填充state
//
func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLine uint64
	//	eg: *3\r\n
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if expectedLine == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = int(expectedLine)
		state.args = make([][]byte, 0, expectedLine)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

//	+Ok\r\n 	-err\r\n 	:5\r\n
// parseSingleLineReply
//  @Description: 客户端也可能发送状态 解析状态响应
//  @param msg
//  @return reply	得到reply类型 类似Ok
//  @return err
//
func parseSingleLineReply(msg []byte) (resp.Reply, error) {
	str := strings.TrimSuffix(string(msg), "\r\n")
	var result resp.Reply
	switch msg[0] {
	case '+':
		result = reply.MakeStatusReply(str[1:])
	case '-':
		result = reply.MakeErrReply(str[1:])
	case ':':
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, errors.New("protocol error: " + string(msg))
		}
		result = reply.MakeIntReply(val)
	}
	return result, nil
}

//	PING\r\n
//
// 	readBody
//  @Description: 实际读出内容和解析 read the non-first lines of multi bulk reply or bulk reply
//  @param msg	eg:$3
//  @param state
//  @return error
//
func readBody(msg []byte, state *readState) error {
	// 去掉\r\n
	line := msg[0 : len(msg)-2]
	var err error
	//$3
	if line[0] == '$' {
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error: " + string(msg))
		}
		// $0\r\n
		if state.bulkLen <= 0 { // null bulk in multi bulks
			state.args = append(state.args, []byte{}) // 空参数
			state.bulkLen = 0
		}
	} else {
		// key
		state.args = append(state.args, line)
	}
	return nil
}
