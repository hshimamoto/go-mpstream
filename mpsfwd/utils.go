// MIT License Copyright (C) 2023 Hiroshi Shimamoto
package main

import (
	"fmt"
)

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
			return fmt.Errorf("readbytes error")
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
			return fmt.Errorf("writebytes error")
		}
		n += w
	}
	return nil
}
