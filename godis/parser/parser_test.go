package parser

import (
	"bytes"
	"github.com/Allen9012/Godis/godis/protocol"
	"github.com/Allen9012/Godis/interface/godis"
	"github.com/Allen9012/Godis/lib/utils"
	"io"
	"testing"
)

/**
  Copyright © 2023 github.com/Allen9012 All rights reserved.
  @author: Allen
  @since: 2023/7/1
  @desc:
  @modified by:
**/

func Test_parse_stream(t *testing.T) {
	replies := []godis.Reply{
		protocol.MakeIntReply(1),
		protocol.MakeStatusReply("OK"),
		protocol.MakeErrReply("ERR unknown"),
		protocol.MakeBulkReply([]byte("a\r\nb")), // test binary safe
		protocol.MakeNullBulkReply(),
		protocol.MakeMultiBulkReply([][]byte{
			[]byte("a"),
			[]byte("\r\n"),
		}),
		protocol.MakeEmptyMultiBulkReply(),
	}

	reqs := bytes.Buffer{}
	for _, re := range replies {
		reqs.Write(re.ToBytes())
	}
	reqs.Write([]byte("set a a" + protocol.CRLF)) // test text protocol
	expected := make([]godis.Reply, len(replies))
	copy(expected, replies)
	expected = append(expected, protocol.MakeMultiBulkReply([][]byte{
		[]byte("set"), []byte("a"), []byte("a"),
	}))
	ch := ParseStream(bytes.NewReader(reqs.Bytes()))
	i := 0
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF {
				return
			}
			t.Error(payload.Err)
			return
		}
		if payload.Data == nil {
			t.Error("empty data")
			return
		}
		exp := expected[i]
		i++
		if !utils.BytesEquals(exp.ToBytes(), payload.Data.ToBytes()) {
			t.Errorf("exp: %s, got: %s", exp, payload.Data)
			t.Error("parse failed: " + string(exp.ToBytes()))
		}
	}
}

func Test_parse_one(t *testing.T) {
	replies := []godis.Reply{
		protocol.MakeIntReply(1),
		protocol.MakeStatusReply("OK"),
		protocol.MakeErrReply("ERR unknown"),
		protocol.MakeBulkReply([]byte("a\r\nb")), // test binary safe
		protocol.MakeNullBulkReply(),
		protocol.MakeMultiBulkReply([][]byte{
			[]byte("a"),
			[]byte("\r\n"),
		}),
		protocol.MakeEmptyMultiBulkReply(),
	}
	for _, re := range replies {
		result, err := ParseOne(re.ToBytes())
		if err != nil {
			t.Error(err)
			continue
		}
		if !utils.BytesEquals(result.ToBytes(), re.ToBytes()) {
			t.Error("parse failed: " + string(re.ToBytes()))
		}
	}
}
