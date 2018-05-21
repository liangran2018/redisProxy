package main

import (
	"github.com/garyburd/redigo/redis"
	"container/list"
	"sync"
	"time"
    "errors"
    "fmt"
)

var NowFunc = time.Now

type ConnPool struct {
	Connect    func() (redis.Conn, error)
	DisConnect func(c *Conn)

	MaxActiveNum int
	ReservedIdleNum int
	IdleTimeout time.Duration
	WaitTime int64

	mu     sync.RWMutex
	closed bool

	idlePool list.List

	activeNum int
}

type Conn struct {
	t    time.Time
	Err  error
	con  redis.Conn
}

func NewConnectionPool(maxActiveNum int, revIdleNum int, idleTimeout time.Duration, waitTime int64, connectFunc func() (redis.Conn, error), disConnectFunc func(c *Conn)) *ConnPool {
	return &ConnPool{
		MaxActiveNum:    maxActiveNum,
		ReservedIdleNum: revIdleNum,
		IdleTimeout:     idleTimeout,
		WaitTime:        waitTime,
		Connect:         connectFunc,
		DisConnect:      disConnectFunc,
	}
}

func (p *ConnPool) Pop() (*Conn, error) {
	var c *Conn
	p.mu.Lock()

	if timeout := p.IdleTimeout; timeout > 0 {
		for i, n := p.ReservedIdleNum, p.idlePool.Len(); i < n; i++ {
			e := p.idlePool.Back()
			if e == nil {
				break
			}
			c := e.Value.(*Conn)
			if c.t.Add(timeout).After(NowFunc()) {
				break
			}
			p.idlePool.Remove(e)
			p.mu.Unlock()
			go p.DisConnect(c)
			p.mu.Lock()
		}
	}

	for {
		if p.idlePool.Len() > 0 {
			e := p.idlePool.Front()
			c = e.Value.(*Conn)
			p.idlePool.Remove(e)

			p.activeNum += 1
			p.mu.Unlock()
			return c, nil
		}

		if p.closed {
			p.mu.Unlock()
			return nil, errors.New("pool closed")
		}

		if p.MaxActiveNum == 0 || p.activeNum < p.MaxActiveNum {
			p.activeNum += 1
			p.mu.Unlock()

			con, e := p.Connect()
			if e != nil {
				p.mu.Lock()
				p.activeNum -= 1
				p.mu.Unlock()
				return nil, fmt.Errorf("connection pool:%s", e.Error())
			}

			c = &Conn{con: con}
			return c, nil
		}

		if p.WaitTime < 0 {
			p.mu.Unlock()
			return nil, nil
		}
	}
}

func (p *ConnPool) Push(c *Conn) error {
	if c == nil {
		return errors.New("connection pool:[Push] c == nil")
	}

	if c.Err != nil {
		p.mu.Lock()
		p.activeNum -= 1
		p.mu.Unlock()
		p.DisConnect(c)
        return nil
    }

	c.t = NowFunc()

	p.mu.Lock()
	p.idlePool.PushFront(c)
	p.activeNum -= 1

	p.mu.Unlock()
    return nil
}

func (p *ConnPool) GetActiveNum() int {
	p.mu.RLock()
	an := p.activeNum
	p.mu.RUnlock()
	return an
}

func (p *ConnPool) GetIdleNum() int {
	p.mu.RLock()
	l := p.idlePool.Len()
	p.mu.RUnlock()
	return l
}
