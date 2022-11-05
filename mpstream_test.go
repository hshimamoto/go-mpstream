// MIT License Copyright (C) 2022 Hiroshi Shimamoto
package mpstream

import "testing"
import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/hshimamoto/go-session"
)

type testLogger struct {
	prefix string
}

func (l *testLogger) Infof(f string, a ...interface{}) {
	pf := fmt.Sprintf("%s:%s", l.prefix, f)
	log.Printf(pf, a...)
}

func (l *testLogger) Debugf(f string, a ...interface{}) {
	pf := fmt.Sprintf("%s:%s", l.prefix, f)
	log.Printf(pf, a...)
}

func TestStream(t *testing.T) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	logger = &testLogger{"TestStream"}
	// server
	done := make(chan struct{})
	serv, err := session.NewServer(":9999", func(conn net.Conn) {
		log.Printf("connected")
		s := NewStream("server")
		s.Add(conn, conn.RemoteAddr().String())
		s.logger = &testLogger{"server"}
		buf := make([]byte, 4096)
		log.Printf("server call read")
		n, _ := s.Read(buf)
		log.Printf("server read %d", n)
		if n != 4 {
			t.Errorf("read failed")
		}
		wseq := s.wseq.Load()
		rseq := s.rseq.Load()
		log.Printf("server %d %d", wseq, rseq)
		n, _ = s.Write(buf[:n])
		log.Printf("server write %d", n)
		wseq = s.wseq.Load()
		rseq = s.rseq.Load()
		log.Printf("server %d %d", wseq, rseq)
		log.Printf("server closing")
		time.Sleep(time.Second)
		s.Close()
		done <- struct{}{}
	})
	if err != nil {
		t.Errorf("NewServer: %v", err)
		return
	}
	log.Printf("start server")
	go serv.Run()
	// client
	cli, err := session.Dial("localhost:9999")
	if err != nil {
		t.Errorf("Dial: %v", err)
		return
	}
	log.Printf("client ok")
	s := NewStream("client")
	s.Add(cli, cli.LocalAddr().String())
	s.logger = &testLogger{"client"}
	n, _ := s.Write([]byte{1, 2, 3, 4})
	log.Printf("client write %d", n)
	if n != 4 {
		t.Errorf("write %d", n)
	}
	n, _ = s.Read(make([]byte, 4096))
	log.Printf("client read %d", n)
	if n != 4 {
		t.Errorf("read %d", n)
	}
	log.Printf("wait server done")
	<-done
	// check
	wseq := s.wseq.Load()
	rseq := s.rseq.Load()
	log.Printf("client %d %d", wseq, rseq)
	s.Close()
}
