// MIT License Copyright (C) 2022 Hiroshi Shimamoto
package mpstream

import (
	"sync"
)

const BufSize = 512 * 1024
const MsgSize = 256 * 1024

// internal ring buffer
type buffer struct {
	m     sync.Mutex
	buf   []byte
	start int
	end   int
}

func newBuffer() *buffer {
	b := &buffer{}
	b.buf = make([]byte, BufSize)
	return b
}

func (b *buffer) push(x []byte) int {
	b.m.Lock()
	defer b.m.Unlock()
	n := len(x)
	rest := BufSize + b.start - b.end
	if rest < n {
		return 0
	}
	n0 := copy(b.buf[b.end%BufSize:], x)
	b.end += n0
	if n0 == n {
		return n
	}
	n1 := copy(b.buf[b.end%BufSize:], x[n0:])
	b.end += n1
	return n
}

func (b *buffer) pop(x []byte) int {
	b.m.Lock()
	defer b.m.Unlock()
	if b.start == b.end {
		return 0
	}
	end := b.end
	if end >= BufSize {
		end = BufSize
	}
	n := copy(x, b.buf[b.start:end])
	b.start += n
	if b.start >= BufSize {
		b.start -= BufSize
		b.end -= BufSize
		if b.end > b.start {
			n1 := copy(x[n:], b.buf[b.start:b.end])
			b.start += n1
			n += n1
		}
	}
	return n
}
