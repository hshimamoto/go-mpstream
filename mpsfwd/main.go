// MIT License Copyright (C) 2022 Hiroshi Shimamoto
package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hshimamoto/go-mpstream"
	"github.com/hshimamoto/go-session"
)

type logger struct {
	prefix string
	debug  bool
}

func (l *logger) Infof(f string, a ...interface{}) {
	pf := fmt.Sprintf("%s:%s", l.prefix, f)
	log.Printf(pf, a...)
}

func (l *logger) Debugf(f string, a ...interface{}) {
	if l.debug == false {
		return
	}
	pf := fmt.Sprintf("%s:%s", l.prefix, f)
	log.Printf(pf, a...)
}

func readbytes(c mpstream.Conn, b []byte) error {
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

func writebytes(c mpstream.Conn, b []byte) error {
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

const BufSize = 48 * 1024

type Connection struct {
	n         int
	connected bool
	conn      net.Conn
	running   bool
}

func (c *Connection) run(addr string, s mpstream.Conn, m *sync.Mutex) {
	conn, err := session.Dial(addr)
	if err != nil {
		return
	}
	log.Printf("connected to %s", addr)
	c.conn = conn
	c.connected = true
	c.running = true
	head := make([]byte, 4)
	buf := make([]byte, BufSize)
	for c.running {
		r, _ := c.conn.Read(buf)
		if r <= 0 {
			c.conn.Close()
			c.connected = false
			c.running = false
			break
		}
		head[0] = 'D'
		head[1] = byte(c.n)
		head[2] = byte(r >> 8)
		head[3] = byte(r)
		m.Lock()
		err0 := writebytes(s, head)
		err1 := writebytes(s, buf[:r])
		m.Unlock()
		if err0 != nil || err1 != nil {
			log.Printf("connection to %s cid=%d: writebytes failed", addr, c.n)
			// stream is dead
			c.conn.Close()
			c.connected = false
			c.running = false
			break
		}
	}
	log.Printf("closing connection cid=%d", c.n)
	head[0] = 'c'
	head[1] = byte(c.n)
	head[2] = 0
	head[3] = 0
	m.Lock()
	writebytes(s, head)
	m.Unlock()
	c.running = false
}

func service(serv *mpstream.Server, s *mpstream.Stream) {
	log.Printf("start service")
	s.SetLogger(&logger{prefix: "server"})
	head := make([]byte, 4)
	buf := make([]byte, BufSize)
	conns := make([]Connection, 256)
	m := &sync.Mutex{}
	for s.IsRunning() {
		if s.NumPaths() > 3 {
			s.RemoveDeadPaths()
		}
		//log.Printf("reading header")
		if readbytes(s, head) != nil {
			// stream is dead
			break
		}
		idx := int(head[1])
		sz := (int(head[2]) << 8) | int(head[3])
		if sz > BufSize {
			// something wrong
			break
		}
		if sz > 0 && readbytes(s, buf[:sz]) != nil {
			// stream is dead
			break
		}
		//log.Printf("get %d", head[0])
		if head[0] == 'C' {
			// new connection
			if conns[idx].running {
				// ignore
				continue
			}
			log.Printf("new connection cid=%d", idx)
			conns[idx].n = idx
			go (&conns[idx]).run(string(buf[:sz]), s, m)
			continue
		}
		if head[0] == 'c' {
			// close connection
			log.Printf("recv close connection cid=%d", idx)
			// force close
			if conns[idx].running {
				conns[idx].conn.Close()
			}
			continue
		}
		// head[0] should be 'D'
		conn := conns[idx]
		if conn.connected == false {
			// just drop
			continue
		}
		// TODO avoid blocking?
		//log.Printf("transfer %d bytes for %d", sz, idx)
		if err := writebytes(conn.conn, buf[:sz]); err != nil {
			// something wrong
			log.Printf("service: %v", err)
			conn.conn.Close()
			conn.connected = false
			conn.running = false
			continue
		}
	}
	log.Printf("service stop")
	serv.Stop()
}

func run_server(addr string) {
	serv, err := mpstream.NewServer(addr, service)
	if err != nil {
		return
	}
	serv.SetLogger(&logger{prefix: "server"})
	serv.Run()
}

func run_client_internal_loop(s *mpstream.Stream, cid int, fwd string, mw *sync.Mutex, conn net.Conn) {
	head := make([]byte, 4)
	buf := make([]byte, BufSize)
	// fwdreq
	sz := len(fwd)
	head[0] = 'C'
	head[1] = byte(cid)
	head[2] = byte(sz >> 8)
	head[3] = byte(sz)
	mw.Lock()
	err0 := writebytes(s, head)
	err1 := writebytes(s, []byte(fwd))
	mw.Unlock()
	if err0 != nil || err1 != nil {
		return
	}
	time.Sleep(100 * time.Millisecond)
	for s.IsRunning() {
		r, _ := conn.Read(buf)
		if r <= 0 {
			break
		}
		head[0] = 'D'
		head[1] = byte(cid)
		head[2] = byte(r >> 8)
		head[3] = byte(r)
		mw.Lock()
		err0 := writebytes(s, head)
		err1 := writebytes(s, buf[:r])
		mw.Unlock()
		if err0 != nil || err1 != nil {
			log.Printf("run_client_internal_loop: bad write")
			break
		}
	}
	log.Printf("run_client_internal_loop: end")
}

func dialpath(addr string) (net.Conn, error) {
	path, err := session.Dial(addr)
	if err != nil {
		log.Printf("dial %v", err)
		return nil, err
	}
	log.Printf("new path <%s>", path.LocalAddr().String())
	return path, nil
}

func run_client_common(fname, listen, addr string, getfwd func(net.Conn) string) {
	cli, err := mpstream.NewClient(addr, 3, dialpath)
	if err != nil {
		log.Printf("NewClient: %v", err)
		return
	}
	s := cli.Stream()
	s.SetLogger(&logger{prefix: "client"})
	m := &sync.Mutex{}
	mw := &sync.Mutex{}
	conns := make([]Connection, 256)
	// okay start localserver
	serv, err := session.NewServer(listen, func(conn net.Conn) {
		defer conn.Close()
		fwd := getfwd(conn)
		// assign new cid
		cid := 256
		m.Lock()
		for i := 0; i < 256; i++ {
			if conns[i].running == false {
				conns[i].conn = conn
				conns[i].connected = true
				conns[i].running = true
				cid = i
				break
			}
		}
		m.Unlock()
		log.Printf("cid=%d", cid)
		if cid == 256 {
			// no cid
			return
		}
		// start reader
		head := make([]byte, 4)
		run_client_internal_loop(s, cid, fwd, mw, conn)
		log.Printf("close cid=%d", cid)
		conns[cid].connected = false
		conns[cid].conn = nil
		head[0] = 'c'
		head[1] = byte(cid)
		head[2] = 0
		head[3] = 0
		mw.Lock()
		writebytes(s, head)
		mw.Unlock()
		log.Printf("[c] sent")
	})
	if err != nil {
		return
	}
	// handle multiplex connection
	go func() {
		head := make([]byte, 4)
		buf := make([]byte, BufSize)
		for s.IsRunning() {
			if readbytes(s, head) != nil {
				// stream closed
				log.Printf("run_client_common: bad header")
				break
			}
			idx := int(head[1])
			sz := (int(head[2]) << 8) | int(head[3])
			if sz > BufSize {
				// something wrong
				break
			}
			if sz > 0 && readbytes(s, buf[:sz]) != nil {
				// stream is dead
				log.Printf("run_client_common: bad data")
				break
			}
			//log.Printf("get %d", head[0])
			if head[0] == 'c' {
				if conns[idx].connected {
					conns[idx].conn.Close()
					conns[idx].connected = false
				}
				conns[idx].running = false
				continue
			}
			if conns[idx].connected == false {
				// unknown id, just ignore
				continue
			}
			// TODO
			//log.Printf("transfer %d bytes for %d", sz, idx)
			if writebytes(conns[idx].conn, buf[:sz]) != nil {
				// something wrong
				log.Printf("run_client_common: writebytes failed")
				break
			}
		}
	}()
	serv.Run()
}

func run_client(listen, addr, fwd string) {
	getfwd := func(conn net.Conn) string {
		return fwd
	}
	run_client_common("run_client", listen, addr, getfwd)
}

func run_proxy(listen, addr string) {
	getfwd := func(conn net.Conn) string {
		// wait CONNECT
		request := make([]byte, 256)
		conn.Read(request)
		a := strings.Split(string(request), " ")
		if len(a) != 3 {
			// bad request
			return ""
		}
		if a[0] != "CONNECT" {
			// bad request
			return ""
		}
		conn.Write([]byte("HTTP/1.0 200 Established\r\n\r\n"))
		fwd := a[1]
		log.Printf("proxy to %s", fwd)
		return fwd
	}
	run_client_common("run_proxy", listen, addr, getfwd)
}

func runcmd(cmd string, args []string) {
	switch cmd {
	case "server":
		run_server(args[0])
		os.Exit(0)
	case "client":
		if len(args) >= 3 {
			run_client(args[0], args[1], args[2])
			os.Exit(0)
		}
	case "proxy":
		if len(args) >= 2 {
			run_proxy(args[0], args[1])
			os.Exit(0)
		}
	}
}

func main() {
	if len(os.Args) >= 3 {
		runcmd(os.Args[1], os.Args[2:])
	}
	fmt.Println("mpsfwd server <addr>")
	fmt.Println("mpsfwd client <listen> <addr> <fwd>")
	fmt.Println("mpsfwd proxy <listen> <addr>")
	os.Exit(1)
}
