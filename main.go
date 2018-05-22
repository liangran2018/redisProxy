package main

import (
	"os"
	"log"
	"flag"
	"compress/zlib"
	"redisProxy/gen-go/proxy"

	"golang.org/x/net/context"
	"github.com/garyburd/redigo/redis"
	"git.apache.org/thrift.git/lib/go/thrift"
)

var (
	redisProxy *RedisProxy
	listenAddr = flag.String("listen", "localhost:13936", "listen thrift address")
	graceful = flag.Bool("graceful", false, "listen on fd open 3 (internal use only)")
)

const (
	defaultTime = 24 * 60 * 60
	maxTime     = 6 * 30 * defaultTime
	redisFile = "server.json"
)

type server struct {}

func main() {
	flag.Parse()

	redisProxy = New(20, "Kci9y3D900", 2, []string{"localhost:2181"}, "/proxy", *listenAddr)

	var err error

	if redisProxy.RedisPool == nil {
		redisProxy.RedisPool = make(map[string][]MasterSlave, 20)
	}

	err = redisProxy.loadPikas(redisFile)
	if err != nil {
		log.Fatalln("loadPikas err:", err)
	}

	err = redisProxy.zkInit()
	if err != nil {
		log.Fatalln("conn zk err:", err)
	}

	err = redisProxy.zkRegister()
	if err != nil {
		log.Println("reg zk err:", err)
	}

	go redisProxy.zkWatch(redisProxy.zkNode)

	handler := &server{}
	processor := proxy.NewServiceProcessor(handler)
	serverTransport, err := thrift.NewTServerSocket(redisProxy.ListenAddr)
	if err != nil {
		log.Fatalln("thrift new socket err:", err)
	}

	transportFactory := thrift.NewTZlibTransportFactoryWithFactory(zlib.BestSpeed, thrift.NewTBufferedTransportFactory(8192))
	protocolFactory := thrift.NewTCompactProtocolFactory()

	s := thrift.NewTSimpleServer4(processor, serverTransport, transportFactory, protocolFactory)
	go redisProxy.HandleSignals(s)

	if *graceful {
		f := os.NewFile(3, "")
		redisProxy.Listener, err = s.FileListener(f)
		if err != nil {
			log.Fatalln("Listen graceful err:", err)
		}
	} else {
		redisProxy.Listener, err = s.Listen()
		if err != nil {
			log.Fatalln("Listen err:", err)
		}
	}

	if err = s.Serve(); err != nil {
		log.Fatalln("serve err:", err)
	}
}

func (s *server) SetUserFeature(ctx context.Context, request *proxy.SetRequest) (r *proxy.SetResponse, err error) {
	if ok := redisProxy.TableIsExist(request.Table); !ok {
		return &proxy.SetResponse{Code: 1, Msg: "tableId not exist"}, nil
	}

	if request.Key == nil {
		return &proxy.SetResponse{Code: 1, Msg: "para err"}, nil
	}

	//默认超时时间为1天，最大超时时间6个月
	if request.Time == 0 {
		request.Time = defaultTime
	} else if request.Time > maxTime {
		request.Time = maxTime
	}

	var conn *Conn
	key, connPool := redisProxy.GetMasterConn(request.Table, request.Key)
	conn, err = connPool.Pop()
	if err != nil {
		return &proxy.SetResponse{Code: 1, Msg: err.Error()}, nil
	}

	_, err = conn.con.Do("SET", key, request.Value, "EX", request.Time)
	if err != nil {
		conn.Err = err
		connPool.Push(conn)
		return &proxy.SetResponse{Code: 1, Msg: err.Error()}, nil
	} else {
		connPool.Push(conn)
		return &proxy.SetResponse{Code: 0, Msg: ""}, nil
	}
}

func (s *server) GetUserFeature(ctx context.Context, request *proxy.GetRequest) (r *proxy.GetResponse, err error) {
	if ok := redisProxy.TableIsExist(request.Table); !ok {
		return &proxy.GetResponse{Code: 1, Msg: "tableId not exist"}, nil
	}

	if request.Key == nil {
		return &proxy.GetResponse{Code: 1, Msg: "para err"}, nil
	}

	var conn *Conn
	key, connPool := redisProxy.GetRandomConn(request.Table, request.Key, request.ForceMaster)
	conn, err = connPool.Pop()
	if err != nil {
		return &proxy.GetResponse{Value: nil, Code: 1, Msg: err.Error()}, nil
	}

	var value []byte
	value, err = redis.Bytes(conn.con.Do("GET", key))
	if err != nil {
		conn.Err = err
		connPool.Push(conn)
		return &proxy.GetResponse{Value: nil, Code: 1, Msg: err.Error()}, nil
	} else {
		connPool.Push(conn)
		return &proxy.GetResponse{Value: value, Code: 0, Msg: ""}, nil
	}
}

func (s *server) HsetUserFeature(ctx context.Context, request *proxy.HsetRequest) (r *proxy.HsetResponse, err error)  {
	if ok := redisProxy.TableIsExist(request.Table); !ok {
		return &proxy.HsetResponse{Code: 1, Msg: "tableId not exist"}, nil
	}

	if request.Key == nil || request.Field == nil {
		return &proxy.HsetResponse{Code: 1, Msg: "para err"}, nil
	}

	var conn *Conn
	key, connPool := redisProxy.GetMasterConn(request.Table, request.Key)
	conn, err = connPool.Pop()
	if err != nil {
		return &proxy.HsetResponse{Code: 1, Msg: err.Error()}, nil
	}

	_, err = conn.con.Do("HSET", key, request.Field, request.Value)
	if err != nil {
		conn.Err = err
		connPool.Push(conn)
		return &proxy.HsetResponse{Code: 1, Msg: err.Error()}, nil
	} else {
		connPool.Push(conn)
		return &proxy.HsetResponse{Code: 0, Msg: ""}, nil
	}
}

func (s *server) HgetUserFeature(ctx context.Context, request *proxy.HgetRequest) (r *proxy.HgetResponse, err error)  {
	if ok := redisProxy.TableIsExist(request.Table); !ok {
		return &proxy.HgetResponse{Code: 1, Msg: "tableId not exist"}, nil
	}

	if request.Key == nil || request.Field == nil {
		return &proxy.HgetResponse{Code: 1, Msg: "para err"}, nil
	}

	var conn *Conn
	key, connPool := redisProxy.GetRandomConn(request.Table, request.Key, request.ForceMaster)
	conn, err = connPool.Pop()
	if err != nil {
		return &proxy.HgetResponse{Value: nil, Code: 1, Msg: err.Error()}, nil
	}

	var value []byte
	value, err = redis.Bytes(conn.con.Do("HGET", key, request.Field))
	if err != nil {
		conn.Err = err
		connPool.Push(conn)
		return &proxy.HgetResponse{Value: nil, Code: 1, Msg: err.Error()}, nil
	} else {
		connPool.Push(conn)
		return &proxy.HgetResponse{Value: value, Code: 0, Msg: ""}, nil
	}
}

func (s *server) HmsetUserFeatures(ctx context.Context, request *proxy.HmsetRequest) (r *proxy.HmsetResponse, err error)  {
	if ok := redisProxy.TableIsExist(request.Table); !ok {
		return &proxy.HmsetResponse{Code: 1, Msg: "tableId not exist"}, nil
	}

	if request.Key == nil || len(request.Values) == 0 {
		return &proxy.HmsetResponse{Code: 1, Msg: "para err"}, nil
	}

	var conn *Conn
	key, connPool := redisProxy.GetMasterConn(request.Table, request.Key)
	conn, err = connPool.Pop()
	if err != nil {
		return &proxy.HmsetResponse{Code: 1, Msg: err.Error()}, nil
	}

	_, err = conn.con.Do("HMSET", redis.Args{}.Add(key).AddFlat(request.Values)...)
	if err != nil {
		conn.Err = err
		connPool.Push(conn)
		return &proxy.HmsetResponse{Code: 1, Msg: err.Error()}, nil
	} else {
		connPool.Push(conn)
		return &proxy.HmsetResponse{Code: 0, Msg: ""}, nil
	}
}

func (s *server) HmgetUserFeatures(ctx context.Context, request *proxy.HmgetRequest) (r *proxy.HmgetResponse, err error)  {
	if ok := redisProxy.TableIsExist(request.Table); !ok {
		return &proxy.HmgetResponse{Code: 1, Msg: "tableId not exist"}, nil
	}

	if request.Key == nil || len(request.Fields) == 0 {
		return &proxy.HmgetResponse{Code: 1, Msg: "para err"}, nil
	}

	var conn *Conn
	key, connPool := redisProxy.GetRandomConn(request.Table, request.Key, request.ForceMaster)
	conn, err = connPool.Pop()
	if err != nil {
		return &proxy.HmgetResponse{Value: nil, Code: 1, Msg: err.Error()}, nil
	}

	var value [][]byte
	value, err = redis.ByteSlices(conn.con.Do("HMGET", redis.Args{}.Add(key).AddFlat(request.Fields)...))
	if err != nil {
		conn.Err = err
		connPool.Push(conn)
		return &proxy.HmgetResponse{Value: nil, Code: 1, Msg: err.Error()}, nil
	} else {
		connPool.Push(conn)
		return &proxy.HmgetResponse{Value: value, Code: 0, Msg: ""}, nil
	}
}
