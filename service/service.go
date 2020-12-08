package service

import (
	"github.com/PMLBlockchain/light_node/pool"
	"github.com/PMLBlockchain/light_node/rpc"
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

	RpcIdGen   uint64
	RpcRequestChan chan *rpc.JSONRpcRequestBundle
	RpcResponseChan chan *rpc.JSONRpcResponse
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
	return conn.Conn.Close()
}

func (conn *WebsocketServiceConn) NextId() uint64 {
	return atomic.AddUint64(&conn.RpcIdGen, 1)
}

var _ ServiceConn = &WebsocketServiceConn{}
