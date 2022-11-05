// MIT License Copyright (C) 2022 Hiroshi Shimamoto
package mpstream

import "testing"

func TestBuffer(t *testing.T) {
	buf := newBuffer()
	x := []byte{1, 2, 3, 4}
	if buf.push(x) != len(x) {
		t.Errorf("push failed")
		return
	}
	y := make([]byte, 4096)
	if buf.pop(y) != len(x) {
		t.Errorf("pop failed")
		return
	}
	if buf.pop(y) != 0 {
		t.Errorf("pop didn't return 0")
		return
	}
	xmax := make([]byte, BufSize)
	for i := 0; i < BufSize; i++ {
		xmax[i] = byte(i % 256)
	}
	if len(xmax) != BufSize {
		t.Errorf("???")
		return
	}
	if buf.push(xmax) != BufSize {
		t.Errorf("push failed with max size")
		return
	}
	ylen := 0
	for {
		n := buf.pop(y)
		if n == 0 {
			break
		}
		for i := 0; i < n; i++ {
			if y[i] != byte((ylen+i)%256) {
				t.Errorf("pop bad contents")
				return
			}
		}
		ylen += n
	}
	if ylen != BufSize {
		t.Errorf("pop failed returns %d", ylen)
		return
	}
}
