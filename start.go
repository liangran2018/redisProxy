package main

import (
	"os"
	"log"
	"net"
	"time"
	"sync"
	"flag"
	"errors"
	"os/exec"
	"syscall"
	"os/signal"
	"io/ioutil"
	"encoding/json"
	"encoding/binary"

	"github.com/samuel/go-zookeeper/zk"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/spaolacci/murmur3"
	"github.com/garyburd/redigo/redis"
)

var (
	listenAddr = flag.String("listen", "localhost:13936", "listen thrift address")
)

const (
	redisFile = "redisServer.json"
)

type redisAddr struct {
	TableId string `json:"tableId"`
	Addr    string `json:"addr"`
}

type MasterSlave struct {
	Master *ConnPool
	Slave  *ConnPool
}

type RedisProxy struct {
	mu sync.Mutex

	RedisPool map[string][]MasterSlave
	redisPass string
	ratio     int

	zkAddr   []string
	zkConn   *zk.Conn
	zkNode   string
	zkIsOpen bool

	signalTrace  chan os.Signal
	proxyIsClose chan bool
	zkWg         sync.WaitGroup

	Listener   net.Listener
	ListenAddr string
}

func Start() *RedisProxy {
	rp := &RedisProxy{}
	var err error

	if rp.RedisPool == nil {
		rp.RedisPool = make(map[string][]MasterSlave, 20)
		err = rp.loadPikas(redisFile)
		if err != nil {
			log.Fatalln("loadPikas err:", err)
		}
	}

	err = rp.zkInit()
	if err != nil {
		log.Fatalln("conn zk err:", err)
	}

	err = rp.zkRegister()
	if err != nil {
		log.Println("reg zk err:", err)
	}

	go rp.zkWatch(rp.zkNode)

	rp.ListenAddr = *listenAddr

	return rp
}

func (rp *RedisProxy) close() {
	signal.Stop(rp.signalTrace)
	rp.zkConn.Close()
	close(rp.proxyIsClose)
}

//conn zk
func (rp *RedisProxy) zkInit() (err error) {
	rp.zkAddr = []string{"localhost:2181"}
	rp.zkConn, _, err = zk.Connect(rp.zkAddr, time.Second * 5)
	if err != nil {
		return err
	} else {
		rp.zkIsOpen = true
		return nil
	}
}

//online reg zk
func (rp *RedisProxy) zkRegister() error {
	rp.zkNode = "/proxy"
	rp.zkConn.Create(rp.zkNode, []byte("proxy info"), 0, zk.WorldACL(zk.PermAll))

	b, _, err := rp.zkConn.Exists("/proxy/" + *listenAddr)
	if err != nil {
		return err
	}

	if !b {
		_, err = rp.zkConn.Create("/proxy/"+*listenAddr, []byte(*listenAddr), 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	}

	return nil
}

func (rp *RedisProxy) loadPikas(filename string) error {
	rp.redisPass = "Kci9y3D900"
	v := make([]redisAddr, 50)
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	err = json.Unmarshal(data, &v)
	if err != nil {
		return err
	}

	for i := 0; i < len(v)>>1; i++ {
		rp.RedisPool[v[i].TableId] = append(rp.RedisPool[v[i].TableId],
			MasterSlave{Master: rp.newPool(v[i].Addr), Slave: rp.newPool(v[i+len(v)>>1].Addr)})
	}

	rp.ratio = 2

	return nil
}

func (rp *RedisProxy) newPool(addr string) *ConnPool {
	return NewConnectionPool(
		4000,
		0,
		60*time.Second,
		3,
		func() (redis.Conn, error) {
			conn, err := redis.Dial("tcp", addr, redis.DialPassword(rp.redisPass))
			if err != nil {
				return nil, err
			}
			return conn, nil
		},

		func(c *Conn) {
			c.con.Close()
		},
	)
}

func (rp *RedisProxy) zkWatch(node string) {
	if rp.proxyIsClose == nil {
		rp.proxyIsClose = make(chan bool)
	}

	for {
		if rp.zkIsOpen != true {
			if err := rp.zkInit(); err != nil {
				log.Fatalln("reconn zk err:", err)
			}
		}

		event := <-rp.proxyIsClose
		rp.zkWg.Add(1)
		if event {
			rp.zkConn.Delete("/proxy/"+*listenAddr, 0)
			rp.zkWg.Done()
		} else {
			rp.zkConn.Create("/proxy/"+*listenAddr, []byte(*listenAddr), 0, zk.WorldACL(zk.PermAll))
			rp.zkWg.Done()
		}
		rp.zkWg.Wait()
	}
}

func getLocalIp() net.IP {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}

	for _, k := range addrs {
		ipnet, ok := k.(*net.IPNet)
		if ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.To4()
			}
		}
	}
	return nil
}

//信号处理
func (rp *RedisProxy) HandleSignals(s *thrift.TSimpleServer) {
	if rp.signalTrace == nil {
		rp.signalTrace = make(chan os.Signal)
	}

	signal.Notify(rp.signalTrace, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR2) //监听收到的信号
	for {
		sig := <-rp.signalTrace
		rp.proxyIsClose <- true
		//SIGUSR2调试信号，使进程可以退出
		if sig == syscall.SIGUSR2 {
			rp.close()
			s.ServerTransport().Close()
		} else {
			err := rp.graceRestart()
			if err != nil {
				log.Fatalln("graceful restart error:", err)
			}
			//p.proxyIsClose <- false
			s.WaitGroup().Wait()
			s.ServerTransport().Close()
		}
	}
}

//拉起子进程
func (rp *RedisProxy) graceRestart() error {
	f, ok := rp.Listener.(*net.TCPListener)
	if !ok {
		return errors.New("graceful restart assert err")
	}

	fi, err := f.File()
	if err != nil {
		return err
	}

	args := []string{"-graceful"}
	cmd := exec.Command(os.Args[0], args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	// put socket FD at the first entry
	cmd.ExtraFiles = []*os.File{fi}

	return cmd.Start()
}

func (rp *RedisProxy) GetRandomConn(tableId string, key []byte, force bool) ([]byte, *ConnPool) {
	k, n := rp.getHashKey(tableId, key)
	if !force && n > uint64(len(rp.RedisPool[tableId])*rp.ratio/10-1) {
		return k, rp.RedisPool[tableId][n].Slave
	} else {
		return k, rp.RedisPool[tableId][n].Master
	}
}

func (rp *RedisProxy) GetMasterConn(tableId string, key []byte) ([]byte, *ConnPool) {
	k, n := rp.getHashKey(tableId, key)
	return k, rp.RedisPool[tableId][n].Master
}

func (rp *RedisProxy) getHashKey(tableId string, key []byte) ([]byte, uint64) {
	buf := make([]byte, len(tableId)+len(key)+16)
	copy(buf, tableId)
	copy(buf[len(tableId):], key)
	h1, h2 := murmur3.Sum128(buf[:len(tableId)+len(key)])

	binary.BigEndian.PutUint64(buf[len(tableId)+len(key):len(tableId)+len(key)+8], h1)
	binary.BigEndian.PutUint64(buf[len(tableId)+len(key)+8:len(tableId)+len(key)+16], h2)
	return buf[len(tableId)+len(key):], h2 % uint64(len(rp.RedisPool[tableId]))
}

func (rp *RedisProxy) TableIsExist(tableId string) bool {
	_, ok := rp.RedisPool[tableId]
	return ok
}
