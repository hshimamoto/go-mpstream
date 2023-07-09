// MIT License Copyright (C) 2023 Hiroshi Shimamoto
package main

import (
	"errors"
	"sync"
)

var EOF = errors.New("EOF")
var EHeader = errors.New("Bad Header")
var EData = errors.New("Bad Data")
var ERead = errors.New("Read Error")
var EWrite = errors.New("Write Error")

type Reader interface {
	Read([]byte) (int, error)
}

type Writer interface {
	Write([]byte) (int, error)
}

func readbytes(c Reader, b []byte) error {
	n := 0
	for n < len(b) {
		r, _ := c.Read(b[n:])
		if r <= 0 {
			return ERead
		}
		n += r
	}
	return nil
}

func writebytes(c Writer, b []byte) error {
	n := 0
	for n < len(b) {
		w, _ := c.Write(b[n:])
		if w <= 0 {
			return EWrite
		}
		n += w
	}
	return nil
}

type RW interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
}

func localToStream(conn RW, s Writer, m *sync.Mutex, cid int, head, buf []byte) error {
	r, _ := conn.Read(buf)
	if r <= 0 {
		return EOF
	}
	head[0] = 'D'
	head[1] = byte(cid)
	head[2] = byte(r >> 8)
	head[3] = byte(r)
	m.Lock()
	err0 := writebytes(s, head)
	err1 := writebytes(s, buf[:r])
	m.Unlock()
	if err0 != nil || err1 != nil {
		return EWrite
	}
	return nil
}

func readFromStream(s Reader, head, buf []byte) (int, error) {
	if readbytes(s, head) != nil {
		return 0, EOF
	}
	sz := (int(head[2]) << 8) | int(head[3])
	if sz > BufSize {
		return 0, EHeader
	}
	if sz > 0 && readbytes(s, buf[:sz]) != nil {
		return 0, EData
	}
	return sz, nil
}
