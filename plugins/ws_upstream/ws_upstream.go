package ws_upstream

import (
	"encoding/json"
	"errors"
	"github.com/PMLBlockchain/light_node/common"
	"github.com/PMLBlockchain/light_node/plugin"
	pluginsCommon "github.com/PMLBlockchain/light_node/plugins/common"
	"github.com/PMLBlockchain/light_node/rpc"
	"github.com/PMLBlockchain/light_node/service"
	"github.com/PMLBlockchain/light_node/utils"
	"github.com/gorilla/websocket"
	"time"
)

var log = utils.GetLogger("upstream")

type WsUpstreamMiddleware struct {
	plugin.MiddlewareAdapter

	pool    service.ServiceConnPool
	options *wsUpstreamMiddlewareOptions
}

func NewWsUpstreamMiddleware(argOptions ...common.Option) *WsUpstreamMiddleware {
	mOptions := &wsUpstreamMiddlewareOptions{
		upstreamTimeout:       30 * time.Second,
		defaultTargetEndpoint: "",
	}
	for _, o := range argOptions {
		o(mOptions)
	}
	return &WsUpstreamMiddleware{
		options: mOptions,
	}
}

func (middleware *WsUpstreamMiddleware) Name() string {
	return "ws-upstream"
}

func (middleware *WsUpstreamMiddleware) OnStart() (err error) {
	log.Info("websocket upstream plugin starting")
	p := service.NewWebsocketServiceConnPool()
	afterConnCreater := func(conn service.ServiceConn) error {
		wsConn, ok := conn.(*service.WebsocketServiceConn)
		if !ok {
			return errors.New("invalid websocket service conn instance")
		}

		log.Infoln("new ws backend connection created")

		c := wsConn.Conn

		// TODO: 初始化login连接
		c.WriteMessage(websocket.TextMessage, []byte("{\"method\":\"call\",\"params\":[1,\"login\",[\"\",\"\"]],\"id\":1}"))
		c.WriteMessage(websocket.TextMessage, []byte("{\"method\":\"call\",\"params\":[1,\"database\",[]],\"id\":2}"))
		c.WriteMessage(websocket.TextMessage, []byte("{\"method\":\"call\",\"params\":[1,\"network_broadcast\",[]],\"id\":3}"))
		c.WriteMessage(websocket.TextMessage, []byte("{\"method\":\"call\",\"params\":[1,\"history\",[]],\"id\":4}"))
		c.WriteMessage(websocket.TextMessage, []byte("{\"method\":\"call\",\"params\":[1,\"network_node\",[]],\"id\":5}"))

		// websocket连接中维护递增的request id
		wsConn.RpcIdGen = 6

		_ = wsConn

		connRequestChan := make(chan *rpc.JSONRpcRequestBundle, 1024)
		connResponseChan := make(chan *rpc.JSONRpcResponse, 1024)

		wsConn.RpcRequestChan = connRequestChan
		wsConn.RpcResponseChan = connResponseChan

		go func() {
			// 读取连接数据放入请求队列，
			defer close(connRequestChan)
			defer close(connResponseChan)

			for {
				messageType, message, err := c.ReadMessage()
				if err != nil {
					log.Warnf("websocket read message error %s\n", err.Error())
					// TODO: 通知连接池关闭并移除这个连接
					return
				}

				var rpcRes *rpc.JSONRpcResponse
				switch messageType {
				case websocket.CloseMessage:
					return
				case websocket.PingMessage:
					continue
				case websocket.PongMessage:
					continue
				case websocket.TextMessage:
					// process target rpc response
					rpcRes, err = rpc.DecodeJSONRPCResponse(message)
					if err != nil {
						return
					}
					if rpcRes == nil {
						err = errors.New("invalid jsonrpc response format from upstream: " + string(message))
						return
					}
					connResponseChan <- rpcRes
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
					rpcData, jsonErr := json.Marshal(reqBundle.Request)
					if jsonErr != nil {
						log.Warnf("json marshal error: %s\n", jsonErr.Error())
						continue
					}
					if e := c.WriteMessage(websocket.TextMessage, rpcData); e != nil {
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

		return nil
	}
	err = p.Init(
		service.MaxConnEachBackend(10),
		service.InitConnEachBackend(0),
		service.AfterConnCreated(afterConnCreater))
	if err != nil {
		return err
	}

	middleware.pool = p

	return middleware.NextOnStart()
}

func (middleware *WsUpstreamMiddleware) getTargetEndpoint(session *rpc.ConnectionSession) (target string, err error) {
	return pluginsCommon.GetSelectedUpstreamTargetEndpoint(session, &middleware.options.defaultTargetEndpoint)
}

func (middleware *WsUpstreamMiddleware) OnConnection(session *rpc.ConnectionSession) (err error) {
	log.Debugf("middleware %s OnConnection called", middleware.Name())
	if session.UpstreamTargetConnection != nil {
		err = errors.New("when OnConnection, session has connected to upstream target before")
		return
	}
	targetEndpoint, err := middleware.getTargetEndpoint(session)
	if err != nil {
		return
	}

	session.UpstreamRpcRequestsChan = make(chan *rpc.JSONRpcRequestBundle, 1000)
	if session.SelectedUpstreamTarget == nil {
		session.SelectedUpstreamTarget = &targetEndpoint
	}

	//这里不用异步，连接获取和初始化如果失败了就立刻返回失败
	log.Debugf("connecting to %s\n", targetEndpoint)

	var connResponseChan chan *rpc.JSONRpcResponse = nil
	err = func() error {

		wsConnWrapper, err := middleware.pool.GetStatelessConn(targetEndpoint)
		if err != nil {
			return err
		}
		log.Infoln("ws backend conn got")
		wsConn, ok := wsConnWrapper.(*service.WebsocketServiceConn)
		if !ok {
			return errors.New("invalid WebsocketServiceConn type")
		}
		session.UpstreamTargetConnection = wsConn

		connResponseChan = wsConn.RpcResponseChan

		log.Debugf("connected to %s\n", targetEndpoint)
		return nil
	}()

	if err != nil {
		return
	}

	session.UpstreamTargetConnectionDone = make(chan struct{})

	go func() {
		defer close(session.UpstreamTargetConnectionDone)

		for {
			select {
			case rpcResponse := <-connResponseChan:
				if rpcResponse == nil {
					log.Debugf("ws backend conn closed")
					// TODO: 可能连接断开了，要返回错误信息给rpcReqChan
					return
				}
				// 如果不是关注的rpc request id，跳过
				rpcRequestId := rpcResponse.Id
				log.Debugf("receive rpc response from ws #%d", rpcRequestId)
				originId, ok := session.SubscribingRequestIds[rpcRequestId]
				if !ok {
					continue
				}
				if rpcReqChan, ok := session.RpcRequestsMap[rpcRequestId]; ok {
					rpcResponse.Id = originId
					rpcReqChan <- rpcResponse
				}
				return
			}
		}

	}()

	return middleware.NextOnConnection(session)
}

func (middleware *WsUpstreamMiddleware) OnConnectionClosed(session *rpc.ConnectionSession) (err error) {
	log.Debugf("middleware %s OnConnectionClosed called", middleware.Name())
	// call next first
	err = middleware.NextOnConnectionClosed(session)

	sessionUpstreamConn := session.UpstreamTargetConnection
	if sessionUpstreamConn != nil {
		wsConn, _ := sessionUpstreamConn.(*service.WebsocketServiceConn)
		log.Infoln("ws backend conn released")
		err = middleware.pool.ReleaseStatelessConn(wsConn)
		if err != nil {
			log.Warnf("release websocket connection error: %s\n", err.Error())
		}
	}
	close(session.UpstreamRpcRequestsChan)
	return
}

func (middleware *WsUpstreamMiddleware) OnTargetWebSocketFrame(session *rpc.ConnectionSession,
	messageType int, message []byte) (next bool, err error) {
	next = true
	requestConnWriteChan := session.RequestConnectionWriteChan
	var rpcRes *rpc.JSONRpcResponse
	switch messageType {
	case websocket.CloseMessage:
		next = false
		requestConnWriteChan <- rpc.NewMessagePack(messageType, message)
	case websocket.PingMessage:
		requestConnWriteChan <- rpc.NewMessagePack(messageType, message)
	case websocket.PongMessage:
		requestConnWriteChan <- rpc.NewMessagePack(messageType, message)
	case websocket.TextMessage:
		// process target rpc response
		rpcRes, err = rpc.DecodeJSONRPCResponse(message)
		if err != nil {
			return
		}
		if rpcRes == nil {
			err = errors.New("invalid jsonrpc response format from upstream: " + string(message))
			return
		}
		rpcRequestId := rpcRes.Id
		if rpcReqChan, ok := session.RpcRequestsMap[rpcRequestId]; ok {
			rpcReqChan <- rpcRes
		}
	}
	return
}

func (middleware *WsUpstreamMiddleware) OnTargetWebsocketError(session *rpc.ConnectionSession, err error) error {
	if utils.IsClosedOrGoingAwayCloseError(err) {
		return nil
	}
	if err == nil {
		return nil
	}
	log.Println("upstream target websocket error:", err)
	return nil
}

func (middleware *WsUpstreamMiddleware) sendRequestToTargetConn(session *rpc.ConnectionSession, messageType int,
	message []byte, rpcRequest *rpc.JSONRpcRequest, rpcResponseFutureChan chan *rpc.JSONRpcResponse) {
	wsConn, _ := session.UpstreamTargetConnection.(*service.WebsocketServiceConn)
	connRequestChan := wsConn.RpcRequestChan
	connRequestChan <- rpc.NewJSONRpcRequestBundle(messageType, message, rpcRequest, rpcResponseFutureChan)
}

func (middleware *WsUpstreamMiddleware) OnWebSocketFrame(session *rpc.JSONRpcRequestSession,
	messageType int, message []byte) (err error) {
	log.Debugf("middleware %s OnWebSocketFrame called", middleware.Name())
	defer func() {
		if err == nil {
			err = middleware.NextOnWebSocketFrame(session, messageType, message)
		}
	}()
	switch messageType {
	case websocket.PingMessage:
		middleware.sendRequestToTargetConn(session.Conn, messageType, message, nil, nil)
	case websocket.PongMessage:
		middleware.sendRequestToTargetConn(session.Conn, messageType, message, nil, nil)
	case websocket.BinaryMessage:
		middleware.sendRequestToTargetConn(session.Conn, messageType, message, nil, nil)
	case websocket.CloseMessage:
		middleware.sendRequestToTargetConn(session.Conn, messageType, message, nil, nil)
	}
	return
}
func (middleware *WsUpstreamMiddleware) OnRpcRequest(session *rpc.JSONRpcRequestSession) (err error) {
	log.Debugf("middleware %s OnRpcRequest called", middleware.Name())
	defer func() {
		if err == nil {
			err = middleware.NextOnJSONRpcRequest(session)
		}
	}()
	connSession := session.Conn

	rpcRequest := session.Request
	rpcRequestBytes := session.RequestBytes

	// create response future before to use in ProcessRpcRequest
	session.RpcResponseFutureChan = make(chan *rpc.JSONRpcResponse, 1)
	if connSession.SelectedUpstreamTarget != nil {
		session.TargetServer = *connSession.SelectedUpstreamTarget
	}

	connSession.RpcRequestsDispatchChannel <- &rpc.RpcRequestDispatchData{
		Type: rpc.RPC_REQUEST_CHANGE_TYPE_ADD_REQUEST,
		Data: session,
	}

	wsConnObj := connSession.UpstreamTargetConnection
	wsConn, _ := wsConnObj.(*service.WebsocketServiceConn)
	rpcRequest.OriginId = rpcRequest.Id
	rpcRequest.Id = wsConn.NextId()

	session.Conn.SubscribingRequestIds[rpcRequest.Id] = rpcRequest.OriginId // TODO: 修改map的并发问题

	middleware.sendRequestToTargetConn(connSession, websocket.TextMessage, rpcRequestBytes, rpcRequest, session.RpcResponseFutureChan)
	return
}
func (middleware *WsUpstreamMiddleware) OnRpcResponse(session *rpc.JSONRpcRequestSession) (err error) {
	log.Debugf("middleware %s OnRpcResponse called", middleware.Name())
	defer func() {
		if err == nil {
			err = middleware.NextOnJSONRpcResponse(session)
		}
	}()
	connSession := session.Conn
	defer func() {
		// notify connSession this rpc response is end
		connSession.RpcRequestsDispatchChannel <- &rpc.RpcRequestDispatchData{
			Type: rpc.RPC_REQUEST_CHANGE_TYPE_ADD_RESPONSE,
			Data: session,
		}
	}()
	responseBytes, jsonErr := json.Marshal(session.Response)
	if jsonErr == nil {
		log.Debugf("upstream response: %s", string(responseBytes))
	}
	return
}

func (middleware *WsUpstreamMiddleware) ProcessRpcRequest(session *rpc.JSONRpcRequestSession) (err error) {
	log.Debugf("middleware %s ProcessRpcRequest called", middleware.Name())
	defer func() {
		if err == nil {
			err = middleware.NextProcessJSONRpcRequest(session)
		}
	}()
	if session.Response != nil {
		return
	}
	rpcRequest := session.Request
	rpcRequestId := rpcRequest.Id
	requestChan := session.RpcResponseFutureChan
	if requestChan == nil {
		err = errors.New("can't find rpc request channel to process")
		return
	}

	var rpcRes *rpc.JSONRpcResponse
	select {
	case <-time.After(middleware.options.upstreamTimeout):
		rpcRes = rpc.NewJSONRpcResponse(rpcRequestId, nil,
			rpc.NewJSONRpcResponseError(rpc.RPC_UPSTREAM_CONNECTION_CLOSED_ERROR,
				"upstream target connection closed", nil))
	case <-session.Conn.UpstreamTargetConnectionDone:
		rpcRes = rpc.NewJSONRpcResponse(rpcRequestId, nil,
			rpc.NewJSONRpcResponseError(rpc.RPC_UPSTREAM_CONNECTION_CLOSED_ERROR,
				"upstream target connection closed", nil))
	case rpcRes = <-requestChan:
		// do nothing, just receive rpcRes
	}
	session.Response = rpcRes
	return
}
