package plugin

import "github.com/PMLBlockchain/light_node/rpc"

type MiddlewareChain struct {
	Middlewares []Middleware
}

func NewMiddlewareChain() *MiddlewareChain {
	return &MiddlewareChain{Middlewares: nil}
}

func (chain *MiddlewareChain) Append(middleware Middleware) *MiddlewareChain {
	chain.Middlewares = append(chain.Middlewares, middleware)
	return chain
}

func (chain *MiddlewareChain) InsertHead(middlewares ...Middleware) *MiddlewareChain {
	addCount := len(middlewares)
	if addCount > 0 {
		count := len(chain.Middlewares)
		items := make([]Middleware, count+addCount)
		for i := 0; i < addCount; i++ {
			items[i] = middlewares[i]
		}
		for i := 0; i < count; i++ {
			items[i+addCount] = chain.Middlewares[i]
		}
		chain.Middlewares = items
	}
	return chain
}

func (chain *MiddlewareChain) OnStart() (err error) {
	for i := 0; i < len(chain.Middlewares); i++ {
		m := chain.Middlewares[i]
		if i > 0 {
			chain.Middlewares[i-1].SetNextMiddleware(m)
		}
		err = m.OnStart()
		if err != nil {
			return
		}
	}
	return
}

func (chain *MiddlewareChain) First() Middleware {
	if len(chain.Middlewares) > 0 {
		return chain.Middlewares[0]
	} else {
		return nil
	}
}

func (chain *MiddlewareChain) OnConnection(session *rpc.ConnectionSession) (err error) {
	first := chain.First()
	if first == nil {
		return nil
	} else {
		log.Debugf("middleware %s OnConnection called", first.Name())
		return first.OnConnection(session)
	}
}

func (chain *MiddlewareChain) OnConnectionClosed(session *rpc.ConnectionSession) (err error) {
	first := chain.First()
	if first == nil {
		return nil
	} else {
		log.Debugf("middleware %s OnConnectionClosed called", first.Name())
		return first.OnConnectionClosed(session)
	}
}

func (chain *MiddlewareChain) OnWebSocketFrame(session *rpc.JSONRpcRequestSession,
	messageType int, message []byte) (err error) {
	first := chain.First()
	if first == nil {
		return nil
	} else {
		log.Debugf("middleware %s OnWebSocketFrame called", first.Name())
		return first.OnWebSocketFrame(session, messageType, message)
	}
}

func (chain *MiddlewareChain) OnJSONRpcRequest(session *rpc.JSONRpcRequestSession) (err error) {
	first := chain.First()
	if first == nil {
		return nil
	} else {
		log.Debugf("middleware %s OnRpcRequest called", first.Name())
		return first.OnRpcRequest(session)
	}
}

func (chain *MiddlewareChain) OnJSONRpcResponse(session *rpc.JSONRpcRequestSession) (err error) {
	first := chain.First()
	if first == nil {
		return nil
	} else {
		log.Debugf("middleware %s OnRpcResponse called", first.Name())
		return first.OnRpcResponse(session)
	}
}

func (chain *MiddlewareChain) ProcessJSONRpcRequest(session *rpc.JSONRpcRequestSession) (err error) {
	first := chain.First()
	if first == nil {
		return nil
	} else {
		log.Debugf("middleware %s ProcessRpcRequest called", first.Name())
		return first.ProcessRpcRequest(session)
	}
}
