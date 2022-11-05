// MIT License Copyright (C) 2022 Hiroshi Shimamoto
package mpstream

import "testing"
import (
	"encoding/binary"
	"log"
	"time"

	"github.com/hshimamoto/go-session"
)

func TestServer(t *testing.T) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	addr := "localhost:9998"
	// start server
	sdone := make(chan struct{})
	serv, err := NewServer(addr, func(serv *Server, s *Stream) {
		s.logger = &testLogger{"server"}
		// echo service
		for s.IsRunning() {
			buf := make([]byte, 256)
			n, _ := s.Read(buf)
			if n == 0 {
				break
			}
			for i := 0; i < n; i++ {
				buf[i] = byte((int(buf[i]) + 1) % 256)
			}
			n, _ = s.Write(buf)
			if n == 0 {
				break
			}
			//log.Printf("echo %d", n)
		}
		sdone <- struct{}{}
	})
	if err != nil {
		t.Errorf("NewServer: %v", err)
		return
	}
	go serv.Run()
	// start client
	cdone := make(chan struct{})
	cli, err := session.Dial(addr)
	if err != nil {
		t.Errorf("Dial: %v", err)
		return
	}
	bufid := make([]byte, 4)
	binary.LittleEndian.PutUint32(bufid[0:], 1)
	cli.Write(bufid)
	s := NewStream("client")
	s.Add(cli, cli.LocalAddr().String())
	s.logger = &testLogger{"client"}
	go func() {
		src := make([]byte, 4096)
		dst := make([]byte, 4096)
		for i := 0; i < len(src); i++ {
			src[i] = byte(i % 256)
		}
		count := 0
		for count < BufSize*2 {
			rest := BufSize*2 - count
			if rest > 4096 {
				rest = 4096
			}
			n, _ := s.Write(src[:rest])
			if n == 0 {
				break
			}
			rest = n
			for rest > 0 {
				r, _ := s.Read(dst)
				if r == 0 {
					break
				}
				for i := 0; i < r; i++ {
					if dst[i] != byte((count+i+1)%256) {
						t.Errorf("response error")
						break
					}
				}
				count += r
				rest -= r
				log.Printf("count=%d (+%d)", count, r)
			}
			if rest > 0 {
				break
			}
			log.Printf("count=%d", count)
		}
		s.Close()
		cdone <- struct{}{}
	}()
	<-cdone
	log.Printf("client done")
	serv.Stop()
	<-sdone
	log.Printf("server done")
}

type slowConn struct {
	Conn
	count int
}

func (c *slowConn) Read(b []byte) (int, error) {
	if c.count > MsgSize {
		time.Sleep(1500 * time.Millisecond)
		c.count = 0
	}
	n, err := c.Conn.Read(b)
	c.count += n
	return n, err
}

func TestServer_mp(t *testing.T) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	addr := "localhost:9997"
	// setup server
	var serv *Server
	sdone := make(chan struct{})
	serv, err := NewServer(addr, func(serv *Server, s *Stream) {
		s.logger = &testLogger{"server"}
		for s.IsRunning() {
			buf := make([]byte, 256)
			n, _ := s.Read(buf)
			if n == 0 {
				break
			}
			for i := 0; i < n; i++ {
				buf[i] = byte((int(buf[i]) + 1) % 256)
			}
			n, _ = s.Write(buf)
			if n == 0 {
				break
			}
		}
		sdone <- struct{}{}
	})
	if err != nil {
		t.Errorf("NewServer: %v", err)
		return
	}
	// setup client
	bufid := make([]byte, 4)
	binary.LittleEndian.PutUint32(bufid[0:], 1)
	// prime path
	conn0, err := session.Dial(addr)
	if err != nil {
		t.Errorf("Dial: %v", err)
		return
	}
	conn0.Write(bufid)
	// second path
	conn1, err := session.Dial(addr)
	if err != nil {
		t.Errorf("Dial: %v", err)
		return
	}
	conn1.Write(bufid)
	// third path
	conn2, err := session.Dial(addr)
	if err != nil {
		t.Errorf("Dial: %v", err)
		return
	}
	conn2.Write(bufid)
	cli := NewStream("client")
	cli.Add(&slowConn{conn0, 0}, conn0.LocalAddr().String())
	cli.Add(&slowConn{conn1, 0}, conn1.LocalAddr().String())
	cli.Add(conn2, conn2.LocalAddr().String())
	cdone := make(chan struct{})

	// start server
	go serv.Run()

	// start client
	go func(s *Stream) {
		s.logger = &testLogger{"client"}
		src := make([]byte, 4096)
		dst := make([]byte, 4096)
		for i := 0; i < len(src); i++ {
			src[i] = byte(i % 256)
		}
		count := 0
		sz := BufSize * 10
		for count < sz {
			rest := sz - count
			if rest > 4096 {
				rest = 4096
			}
			n, _ := s.Write(src[:rest])
			if n == 0 {
				break
			}
			rest = n
			for rest > 0 {
				r, _ := s.Read(dst)
				if r == 0 {
					break
				}
				for i := 0; i < r; i++ {
					if dst[i] != byte((count+i+1)%256) {
						t.Errorf("response error")
						break
					}
				}
				count += r
				rest -= r
				log.Printf("count=%d (+%d)", count, r)
			}
			if rest > 0 {
				break
			}
			log.Printf("count=%d", count)
		}
		s.Close()
		cdone <- struct{}{}
	}(cli)
	<-cdone
	log.Printf("client done")
	serv.Stop()
	<-sdone
	log.Printf("server done")
}
