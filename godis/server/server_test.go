package server

import (
	"bufio"
	"github.com/Allen9012/Godis/tcp"
	"net"
	"testing"
	"time"
)

/**
  Copyright © 2023 github.com/Allen9012 All rights reserved.
  @author: Allen
  @since: 2023/6/27
  @desc:
  @modified by:
**/

func TestListenAndServe(t *testing.T) {
	var err error
	closeChan := make(chan struct{})
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Error(err)
		return
	}
	addr := listener.Addr().String()
	go tcp.ListenAndServe(listener, MakeHandler(), closeChan)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = conn.Write([]byte("PING\r\n"))
	if err != nil {
		t.Error(err)
		return
	}
	bufReader := bufio.NewReader(conn)
	line, _, err := bufReader.ReadLine()
	if err != nil {
		t.Error(err)
		return
	}
	if string(line) != "+PONG" {
		t.Error("get wrong response")
		return
	}
	closeChan <- struct{}{}
	time.Sleep(time.Second)
}
