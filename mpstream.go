// MIT License Copyright (C) 2022 Hiroshi Shimamoto
package mpstream

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Conn interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
	Close() error
}

type UpcallFunc func([]byte) error
type AckedFunc func(uint32)

type Path struct {
	conn Conn
	name string
	// logging
	logger Logger
	// for worker
	running bool
	// sync writes
	m        sync.Mutex
	ackmsg   []byte
	wack     *atomic.Uint32
	rack     *atomic.Uint32
	upcall   UpcallFunc
	acked    AckedFunc
	inflight bool
	//
	mLastRcv sync.Mutex
	lastRecv time.Time
	mLastAck sync.Mutex
	lastAck  time.Time
	dead     bool
}

func newPath(name string, conn Conn, upcall UpcallFunc, acked AckedFunc) *Path {
	p := &Path{name: name, conn: conn}
	p.running = true
	p.ackmsg = []byte{0xff, 0, 0, 4, 0, 0, 0, 0}
	p.wack = &atomic.Uint32{}
	p.wack.Store(0)
	p.rack = &atomic.Uint32{}
	p.rack.Store(0)
	p.upcall = upcall
	p.acked = acked
	p.logger = logger
	p.lastRecv = time.Now()
	go path_reader(p)
	go path_writer(p)
	go path_checker(p)
	return p
}

func (p *Path) Infof(f string, a ...interface{}) {
	prefix := fmt.Sprintf("Path<%v>:", p.name)
	p.logger.Infof(prefix+f, a...)
}

func (p *Path) Debugf(f string, a ...interface{}) {
	prefix := fmt.Sprintf("Path<%v>:", p.name)
	p.logger.Debugf(prefix+f, a...)
}

func (p *Path) readBytes(b []byte) error {
	sz := len(b)
	ptr := 0
	for ptr < sz {
		n, _ := p.conn.Read(b[ptr:sz])
		if n == 0 {
			return errors.New("closed")
		}
		ptr += n
	}
	return nil
}

func (p *Path) writeBytes(b []byte) error {
	sz := 0
	for sz < len(b) {
		n, _ := p.conn.Write(b)
		if n <= 0 {
			return errors.New("write failed")
		}
		sz += n
	}
	return nil
}

func (p *Path) writeAck() error {
	ack := p.wack.Load()
	p.m.Lock()
	defer p.m.Unlock()
	binary.LittleEndian.PutUint32(p.ackmsg[4:], ack)
	return p.writeBytes(p.ackmsg)
}

func (p *Path) writeMsg(b []byte) error {
	p.m.Lock()
	defer p.m.Unlock()
	p.inflight = true
	return p.writeBytes(b)
}

func (p *Path) Close() error {
	p.running = false
	return p.conn.Close()
}

func path_reader(p *Path) {
	buf := make([]byte, BufSize)
	for p.running {
		// header
		if err := p.readBytes(buf[0:4]); err != nil {
			p.Debugf("reading header: closed")
			break
		}
		// header OK
		m0 := int(buf[0])
		sz := (int(buf[1]) << 16) | (int(buf[2]) << 8) | int(buf[3])
		if sz < 4 || sz > MsgSize {
			// bad...
			p.Debugf("bad %d", sz)
			break
		}
		// body
		if err := p.readBytes(buf[0:sz]); err != nil {
			p.Debugf("reading body: closed")
			break
		}
		// ok
		p.mLastRcv.Lock()
		p.lastRecv = time.Now()
		p.dead = false
		p.mLastRcv.Unlock()
		// handle message
		if m0 == 0xff {
			// status update
			ack := binary.LittleEndian.Uint32(buf[0:])
			p.rack.Store(ack)
			p.inflight = false
			p.acked(ack)
			// handled
			continue
		}
		if p.upcall(buf[:sz]) != nil {
			// upcall failed
			time.Sleep(time.Second)
			p.Debugf("upcall failed")
			continue
		}
	}
	p.Infof("path_reader: done")
}

func path_writer(p *Path) {
	prevack := p.wack.Load()
	lastAck := time.Now()
	for p.running {
		now := time.Now()
		ack := p.wack.Load()
		if prevack != ack || now.Sub(lastAck) > 5*time.Second {
			p.writeAck()
			prevack = ack
			lastAck = now
		}
		p.mLastAck.Lock()
		p.lastAck = time.Now()
		p.mLastAck.Unlock()
		time.Sleep(time.Second)
	}
	p.Infof("path_writer: done")
}

func path_checker(p *Path) {
	for p.running {
		now := time.Now()
		// check Last Recv
		p.mLastRcv.Lock()
		if now.Sub(p.lastRecv) > time.Minute {
			// communication stopped
			p.dead = true
		}
		p.mLastRcv.Unlock()
		if p.dead {
			break
		}
		// check Last Ack
		p.mLastAck.Lock()
		if now.Sub(p.lastAck) > time.Minute {
			// communication stopped
			p.dead = true
		}
		p.mLastAck.Unlock()
		if p.dead {
			break
		}
		time.Sleep(3 * time.Second)
	}
	p.Close()
	p.Infof("path_checker: done")
}

type waiter struct {
	num *atomic.Uint32
	ch  chan struct{}
}

func newWaiter() *waiter {
	w := &waiter{
		num: &atomic.Uint32{},
		ch:  make(chan struct{}),
	}
	w.num.Store(0)
	return w
}

func (w *waiter) wait() {
	w.num.Store(1)
	<-w.ch
}

func (w *waiter) kick() {
	if w.num.Swap(0) == 0 {
		return
	}
	w.ch <- struct{}{}
}

// Stream
type Stream struct {
	name string
	// path handling
	paths []*Path
	prime *Path
	mPath sync.Mutex
	// controll
	logger  Logger
	running bool
	done    chan struct{}
	worker  *waiter
	// read/write buffer
	wseq  *atomic.Uint32
	rseq  *atomic.Uint32
	wbuf  *buffer
	rbuf  *buffer
	wwait *waiter
	rwait *waiter
	mrbuf sync.Mutex
	// sending data
	mpending   sync.Mutex
	pending    []byte
	pendinglen int
	sendcount  int
	sendtime   time.Time
}

func NewStream(name string) *Stream {
	s := &Stream{}
	s.name = name
	s.logger = logger
	s.running = true
	s.done = make(chan struct{})
	s.worker = newWaiter()
	s.wseq = &atomic.Uint32{}
	s.wseq.Store(0)
	s.rseq = &atomic.Uint32{}
	s.rseq.Store(0)
	s.wbuf = newBuffer()
	s.rbuf = newBuffer()
	s.wwait = newWaiter()
	s.rwait = newWaiter()
	s.pending = make([]byte, MsgSize)
	s.pendinglen = 0
	// start worker
	go worker(s)
	go ticker(s)
	return s
}

func (s *Stream) Add(conn Conn, name string) {
	s.mPath.Lock()
	p := newPath(name, conn, s.upcall, s.acked)
	p.logger = logger
	s.paths = append(s.paths, p)
	if s.prime == nil {
		s.prime = p
	}
	s.mPath.Unlock()
}

func (s *Stream) RemoveDeadPaths() {
	s.mPath.Lock()
	paths := s.paths
	s.paths = []*Path{}
	for _, path := range paths {
		if path == s.prime || path.dead == false {
			s.paths = append(s.paths, path)
			continue
		}
		s.Infof("remove path: %s", path.name)
		path.Close()
	}
	s.mPath.Unlock()
}

func (s *Stream) NumPaths() int {
	s.mPath.Lock()
	defer s.mPath.Unlock()
	return len(s.paths)
}

func (s *Stream) IsRunning() bool {
	return s.running
}

func (s *Stream) upcall(b []byte) error {
	s.Debugf("upcall len=%d acked=%d", len(b), s.rseq.Load())
	if len(b) < 4 {
		// discard
		return nil
	}
	// unmask data
	for i := 0; i < len(b); i++ {
		b[i] ^= 0x66
	}
	seq := binary.LittleEndian.Uint32(b[:4])
	// avoid pushing race
	s.mrbuf.Lock()
	defer s.mrbuf.Unlock()
	if s.rseq.Load() != seq {
		// discard
		return nil
	}
	// try to push
	ret := s.rbuf.push(b[4:])
	if ret == 0 {
		return errors.New("not enough space")
	}
	s.rwait.kick()
	ack := seq + uint32(ret)
	s.rseq.Store(ack)
	//s.logger.Infof("ack=%d", ack)
	s.mPath.Lock()
	for _, p := range s.paths {
		p.wack.Store(ack)
	}
	s.mPath.Unlock()
	s.prime.writeAck()
	s.worker.kick()
	return nil
}

func (s *Stream) acked(ack uint32) {
	s.mpending.Lock()
	defer s.mpending.Unlock()
	seq := s.wseq.Load() + uint32(s.pendinglen)
	if ack == seq {
		s.wseq.Store(ack)
		s.Debugf("acked ack=%d wseq=%d", ack, seq)
		s.wwait.kick()
		s.pendinglen = 0
		s.refill()
		s.sendNext()
	}
}

// call with s.mpending Locked
func (s *Stream) refill() {
	if s.pendinglen > 0 {
		return
	}
	s.pendinglen = s.wbuf.pop(s.pending[8:MsgSize])
	if s.pendinglen == 0 {
		return
	}
	s.worker.kick()
	seq := s.wseq.Load()
	sz := s.pendinglen + 4
	// create header
	s.pending[0] = 0xfe
	s.pending[1] = byte((sz >> 16) & 0xff)
	s.pending[2] = byte((sz >> 8) & 0xff)
	s.pending[3] = byte(sz & 0xff)
	// set seq
	binary.LittleEndian.PutUint32(s.pending[4:], seq)
	s.sendcount = 0
	// mask data
	for i := 4; i < sz+4; i++ {
		s.pending[i] ^= 0x66
	}
}

// call with s.mpending Locked
func (s *Stream) sendNext() {
	if s.pendinglen == 0 {
		return
	}
	now := time.Now()
	if s.sendcount > 0 {
		if now.Sub(s.sendtime) < 100*time.Millisecond {
			return
		}
		// change prime
		s.mPath.Lock()
		for _, p := range s.paths {
			if !p.inflight {
				s.Infof("select prime %s", p.name)
				s.prime = p
				break
			}
		}
		s.mPath.Unlock()
	}
	s.Debugf("pendinglen=%d sendcount=%d", s.pendinglen, s.sendcount)
	s.prime.writeMsg(s.pending[:s.pendinglen+8])
	s.sendcount++
	s.sendtime = now
}

func (s *Stream) Close() error {
	s.mPath.Lock()
	for _, p := range s.paths {
		p.Close()
	}
	s.mPath.Unlock()
	s.running = false
	s.worker.kick()
	s.wwait.kick()
	s.rwait.kick()
	// done from worker and ticker
	<-s.done
	<-s.done
	return nil
}

func (s *Stream) Write(b []byte) (int, error) {
	var ret int = 0
	for s.running {
		n := len(b)
		if n > MsgSize {
			n = MsgSize
		}
		ret = s.wbuf.push(b[:n])
		if ret > 0 {
			break
		}
		s.Debugf("Write wait")
		if !s.running {
			break
		}
		s.worker.kick()
		s.wwait.wait()
	}
	s.worker.kick()
	return ret, nil
}

func (s *Stream) Read(b []byte) (int, error) {
	var ret int = 0
	for s.running {
		ret = s.rbuf.pop(b)
		if ret > 0 {
			break
		}
		s.Debugf("Read wait")
		if !s.running {
			break
		}
		s.worker.kick()
		s.rwait.wait()
	}
	s.worker.kick()
	return ret, nil
}

func (s *Stream) SetLogger(l Logger) {
	s.logger = l
	s.mPath.Lock()
	for _, p := range s.paths {
		p.logger = l
	}
	s.mPath.Unlock()
}

func (s *Stream) Infof(f string, a ...interface{}) {
	prefix := fmt.Sprintf("%s:", s.name)
	s.logger.Infof(prefix+f, a...)
}

func (s *Stream) Debugf(f string, a ...interface{}) {
	prefix := fmt.Sprintf("%s:", s.name)
	s.logger.Debugf(prefix+f, a...)
}

func worker(s *Stream) {
	for s.running {
		s.worker.wait()
		s.mpending.Lock()
		s.refill()
		s.sendNext()
		s.mpending.Unlock()
	}
	s.done <- struct{}{}
}

func ticker(s *Stream) {
	t := time.NewTicker(time.Second)
	for s.running {
		select {
		case <-t.C:
		}
		s.worker.kick()
		s.wwait.kick()
		s.rwait.kick()
	}
	t.Stop()
	s.done <- struct{}{}
}
