// MIT License Copyright (C) 2022 Hiroshi Shimamoto
package mpstream

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"

	"github.com/hshimamoto/go-session"
)

type ServiceFunc func(*Server, *Stream)

type Server struct {
	serv    *session.Server
	streams map[uint32]*Stream
	m       sync.Mutex
	logger  Logger
}

func NewServer(addr string, service ServiceFunc) (*Server, error) {
	s := &Server{}
	s.logger = logger
	s.streams = map[uint32]*Stream{}
	serv, err := session.NewServer(addr, func(conn net.Conn) {
		buf := make([]byte, 4)
		n, _ := conn.Read(buf)
		if n < 4 {
			conn.Close()
			return
		}
		id := binary.LittleEndian.Uint32(buf[0:])
		raddr := conn.RemoteAddr().String()
		s.logger.Infof("accept from <%s> cid=%d", raddr, id)
		s.m.Lock()
		st, ok := s.streams[id]
		if ok {
			if st.IsRunning() {
				st.Add(conn, raddr)
				s.logger.Infof("cid=%d path add", id)
			} else {
				conn.Close()
				s.logger.Infof("cid=%d not running", id)
			}
		} else {
			st = NewStream(fmt.Sprintf("<%x>", id))
			st.Add(conn, raddr)
			s.logger.Infof("new stream")
			s.streams[id] = st
		}
		s.m.Unlock()
		if ok {
			return
		}
		go service(s, st)
	})
	if err != nil {
		return nil, err
	}
	s.serv = serv
	return s, nil
}

func (s *Server) Run() {
	s.serv.Run()
}

func (s *Server) Stop() {
	s.serv.Stop()
	// then close Stream
	for _, st := range s.streams {
		st.Close()
	}
}

func (s *Server) SetLogger(l Logger) {
	s.logger = l
}
