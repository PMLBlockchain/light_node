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

	err = p.Init(
		service.MaxConnEachBackend(10),
		service.InitConnEachBackend(0),
		service.AfterConnCreated(WsConnInitCallback))
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

	go func() {

		for {
			select {
			case rpcResponse := <-connResponseChan:
				if rpcResponse == nil {
					log.Debugf("ws backend conn closed")
					// 连接关闭了，通知返回失败信息
					for _, rpcReqChan := range session.RpcRequestsMap {
						close(rpcReqChan)
					}
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
			case <- time.After(middleware.options.upstreamTimeout):
				for _, rpcReqChan := range session.RpcRequestsMap {
					close(rpcReqChan)
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
		err = middleware.pool.ReleaseStatelessConn(wsConn, wsConn.Closed)
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
		// 返回信息已经被其他middleware写入过了，不需要再处理
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
	case rpcRes = <-requestChan:
		// do nothing, just receive rpcRes
		if rpcRes == nil {
			rpcRes = rpc.NewJSONRpcResponse(rpcRequestId, nil,
				rpc.NewJSONRpcResponseError(rpc.RPC_UPSTREAM_TIMEOUT_ERROR,
					"upstream target connection timeout or closed", nil))
		}
	}
	session.Response = rpcRes
	return
}
