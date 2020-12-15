package service

import (
	"github.com/PMLBlockchain/light_node/pool"
	"github.com/gorilla/websocket"
	"io"
	"sync/atomic"
)

type ServiceConn interface {
	io.Closer
	IsStateful() bool
	GetServiceKey() string
	GetPoolableConn() *pool.PoolableProxy
	SetPoolableConn(c *pool.PoolableProxy)
	GetSessionId() string
	SetSessionId(sessId string)
}

type WebsocketServiceConn struct {
	Stateful     bool
	Endpoint     string
	ServiceKey   string
	Conn         *websocket.Conn
	PoolableConn *pool.PoolableProxy
	SessionId    string
	Closed bool

	RpcIdGen   uint32 // 只能用uint32，因为uint64的atomic操作不支持32位系统
	RpcRequestChan chan []byte
	RpcResponseChan chan []byte
}

func (conn *WebsocketServiceConn) IsStateful() bool {
	return conn.Stateful
}

func (conn *WebsocketServiceConn) GetServiceKey() string {
	return conn.ServiceKey
}

func (conn *WebsocketServiceConn) GetPoolableConn() *pool.PoolableProxy {
	return conn.PoolableConn
}

func (conn *WebsocketServiceConn) SetPoolableConn(c *pool.PoolableProxy) {
	conn.PoolableConn = c
}

func (conn *WebsocketServiceConn) GetSessionId() string {
	return conn.SessionId
}
func (conn *WebsocketServiceConn) SetSessionId(sessId string) {
	conn.SessionId = sessId
}

func (conn *WebsocketServiceConn) Close() error {
	defer func() {
		conn.Closed = true
	}()
	return conn.Conn.Close()
}

func (conn *WebsocketServiceConn) SetRpcIdGen(value uint32) {
	for {
		if conn.RpcIdGen == value {
			return
		}
		if atomic.CompareAndSwapUint32(&conn.RpcIdGen, conn.RpcIdGen, value) {
			return
		}
	}
}

func (conn *WebsocketServiceConn) NextId() uint64 {
	return uint64(atomic.AddUint32(&conn.RpcIdGen, 1))
}

var _ ServiceConn = &WebsocketServiceConn{}
