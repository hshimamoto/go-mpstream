// MIT License Copyright (C) 2023 Hiroshi Shimamoto
package mpstream

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"os"
	"time"
)

type Client struct {
	id   uint32
	addr string
	nr   int
	dial func(string) (net.Conn, error)
	s    *Stream
}

func NewClient(addr string, nr int, dial func(string) (net.Conn, error)) (*Client, error) {
	if nr < 1 {
		return nil, fmt.Errorf("bad arg")
	}
	rand.Seed(time.Now().Unix())
	id := rand.Uint32() * uint32(os.Getpid()) // random client id
	c := &Client{
		id:   id,
		addr: addr,
		nr:   nr,
		dial: dial,
	}
	err := c.start()
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) dialPath() (net.Conn, error) {
	conn, err := c.dial(c.addr)
	if err != nil {
		return nil, err
	}
	bufid := make([]byte, 4)
	binary.LittleEndian.PutUint32(bufid[0:], c.id)
	conn.Write(bufid) // send client id
	return conn, nil
}

func (c *Client) start() error {
	// first path
	path, err := c.dialPath()
	if err != nil {
		return err
	}
	name := fmt.Sprintf("client:%d", c.id)
	s := NewStream(name)
	s.Add(path, path.LocalAddr().String())
	go func() {
		prev := 0
		for s.IsRunning() {
			curr := s.NumPaths()
			if prev != curr {
				prev = curr
			}
			if curr == 0 {
				// lose all links
				s.Close()
				return
			}
			if curr < c.nr {
				path, err := c.dialPath()
				if err != nil {
					// ignore
					continue
				}
				s.Add(path, path.LocalAddr().String())
				time.Sleep(time.Second)
				continue
			}
			s.RemoveDeadPaths()
			time.Sleep(time.Minute)
		}
	}()
	c.s = s
	return nil
}

func (c *Client) Stream() *Stream {
	return c.s
}
