package service

import (
	"github.com/PMLBlockchain/light_node/common"
	"github.com/PMLBlockchain/light_node/pool"
	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type websocketServiceConnPool struct {
	options *wsServiceConnPoolOptions

	// 每个backend service都有一个pool.ConnPool对象. pool初始大小0，
	connPools    map[string]pool.ConnPool
	connSessions *sync.Map // 如果是有状态会话，sessionId => ServiceConn的映射
}

func NewWebsocketServiceConnPool() ServiceConnPool {
	return &websocketServiceConnPool{
		connPools:    make(map[string]pool.ConnPool),
		connSessions: &sync.Map{},
	}
}

func (pool *websocketServiceConnPool) Init(argOptions ...common.Option) error {
	options := &wsServiceConnPoolOptions{
		maxConnEachBackend:  10,
		initConnEachBackend: 0,
	}
	for _, o := range argOptions {
		o(options)
	}
	pool.options = options
	return nil
}

func (p *websocketServiceConnPool) getOrCreateConnPool(targetServiceKey string) (result pool.ConnPool, err error) {
	result, ok := p.connPools[targetServiceKey]
	if ok {
		return
	}
	connCreator := func() (conn pool.Poolable, err error) {
		// 创建到backend的连接
		// targetServiceKey 就是目标endpoint
		targetEndpoint := targetServiceKey
		c, _, err := websocket.DefaultDialer.Dial(targetEndpoint, nil)
		if err != nil {
			return
		}
		serviceConn := &WebsocketServiceConn{
			Stateful:     true,
			Endpoint:     targetEndpoint,
			ServiceKey:   targetServiceKey,
			Conn:         c,
			PoolableConn: nil,
		}

		connRequestChan := make(chan []byte, 1024)
		connResponseChan := make(chan []byte, 1024)

		serviceConn.RpcRequestChan = connRequestChan
		serviceConn.RpcResponseChan = connResponseChan

		go func() {
			// 读取连接数据放入请求队列，
			defer close(connRequestChan)
			defer close(connResponseChan)
			defer func() {
				// 关闭连接并且在连接池中移除这个连接
				serviceConn.Close()
			}()

			for {
				messageType, message, err := c.ReadMessage()
				if err != nil {
					log.Warnf("websocket read message error %s\n", err.Error())
					return
				}

				switch messageType {
				case websocket.CloseMessage:
					return
				case websocket.PingMessage:
					continue
				case websocket.PongMessage:
					continue
				case websocket.TextMessage:
					connResponseChan <- message
				}
			}

		}()
		go func() {
			// select消息，把请求队列数据写入连接
			for {
				select {
				case reqBundle := <-connRequestChan:
					if reqBundle == nil {
						return // closed
					}
					if e := c.WriteMessage(websocket.TextMessage, reqBundle); e != nil {
						log.Warnf("websocket write message error: %s\n", e.Error())
						// TODO: 通知连接池关闭并移除这个连接
						return
					}
				case <-time.After(10 * time.Second):
					if e := c.WriteMessage(websocket.PingMessage, []byte("ping")); e != nil {
						log.Warnf("websocket write message error: %s\n", e.Error())
						return
					}
				}
			}
		}()


		if p.options.afterConnCreated != nil {
			err = p.options.afterConnCreated(serviceConn)
			if err != nil {
				return
			}
		}


		conn = serviceConn
		return
	}
	newConnPool, err := pool.NewConnPool(p.options.maxConnEachBackend, p.options.initConnEachBackend, connCreator)
	if err != nil {
		return
	}
	p.connPools[targetServiceKey] = newConnPool
	result = newConnPool
	return
}

func getRealConn(connWrap *pool.PoolableProxy) ServiceConn {
	conn, ok := connWrap.Real().(ServiceConn)
	if !ok {
		panic("invalid real conn type in ws pool")
	}
	return conn
}

func (p *websocketServiceConnPool) GetStatelessConn(targetServiceKey string) (conn ServiceConn, err error) {
	connPool, err := p.getOrCreateConnPool(targetServiceKey)
	if err != nil {
		return
	}
	connWrap, err := connPool.Get()
	if err != nil {
		return
	}
	conn = getRealConn(connWrap)
	conn.SetPoolableConn(connWrap)
	return
}

func (p *websocketServiceConnPool) ReleaseStatelessConn(conn ServiceConn, removeIt bool) (err error) {
	connPool, err := p.getOrCreateConnPool(conn.GetServiceKey())
	if err != nil {
		return
	}
	if removeIt {
		err = connPool.Remove(conn.GetPoolableConn())
	} else {
		err = connPool.GiveBack(conn.GetPoolableConn())
	}
	return
}

func (p *websocketServiceConnPool) GetStatefulConn(targetServiceKey string,
	sessionId string, reuse bool) (conn ServiceConn, err error) {
	sessionConn, ok := p.connSessions.Load(sessionId)
	if ok {
		conn = sessionConn.(ServiceConn)
		return
	}
	connPool, err := p.getOrCreateConnPool(targetServiceKey)
	if err != nil {
		return
	}
	connWrap, err := connPool.Get()
	if err != nil {
		return
	}
	conn = getRealConn(connWrap)
	conn.SetPoolableConn(connWrap)
	conn.SetSessionId(sessionId)
	p.connSessions.Store(sessionId, conn)
	return
}

func (p *websocketServiceConnPool) ReleaseStatefulConn(conn ServiceConn) (err error) {
	sessionId := conn.GetSessionId()
	connPool, err := p.getOrCreateConnPool(conn.GetServiceKey())
	if err != nil {
		return
	}
	// 有状态连接不仅要归还还要关闭连接，因为不能被复用了
	err = connPool.GiveBack(conn.GetPoolableConn())
	if err != nil {
		return
	}
	p.connSessions.Delete(sessionId)
	return
}

func (p *websocketServiceConnPool) Shutdown() error {
	var err error
	for _, connPool := range p.connPools {
		// 关闭连接池中各连接
		err1 := connPool.Close()
		if err1 != nil && err == nil {
			err = err1
		}
	}
	p.connPools = nil
	return err
}
