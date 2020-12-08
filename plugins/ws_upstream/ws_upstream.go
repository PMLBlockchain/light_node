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

	pool service.ServiceConnPool
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
	afterConnCreater := func (conn service.ServiceConn) error {
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

		// websocket连接中维护递增的request id
		wsConn.RpcIdGen = 5


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
				case reqBundle := <- connRequestChan:
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
						return
					}
				case <- time.After(10 * time.Second):
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

func connectTargetEndpoint(targetEndpoint string) (*websocket.Conn, error) {
	conn, _, err := websocket.DefaultDialer.Dial(targetEndpoint, nil)
	return conn, err
}

func (middleware *WsUpstreamMiddleware) OnConnection(session *rpc.ConnectionSession) (err error) {
	// TODO: upstream连接可以设置每个连接新建一个到upstream的连接，也可以选择从upstream connection pool中选择一个满足target的连接复用
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
	session.ConnectionInitedChan = make(chan interface{}, 0)

	go func() {
		log.Debugf("connecting to %s\n", targetEndpoint)

		// TODO: 用连接池
		sessionId := "0"
		wsConnWrapper, err := middleware.pool.GetStatefulConn(targetEndpoint, sessionId, true)
		//c, err := connectTargetEndpoint(targetEndpoint)
		if err != nil {
			log.Println("dial:", err)
			return
		}
		log.Infoln("ws backend conn got")
		wsConn, ok := wsConnWrapper.(*service.WebsocketServiceConn)
		if !ok {
			log.Errorln("invalid WebsocketServiceConn type")
			return
		}
		session.UpstreamTargetConnection = wsConn

		connResponseChan := wsConn.RpcResponseChan

		log.Debugf("connected to %s\n", targetEndpoint)

		session.UpstreamTargetConnectionDone = make(chan struct{})
		defer func() {
			close(session.UpstreamTargetConnectionDone)
		}()

		close(session.ConnectionInitedChan)

		for {
			select {
				case rpcResponse := <- connResponseChan:
					if rpcResponse == nil {
						// TODO: 可能连接断开了，要返回错误信息给rpcReqChan
						return
					}
					// 如果不是关注的rpc request id，跳过
					rpcRequestId := rpcResponse.Id
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
	// call next first
	err = middleware.NextOnConnectionClosed(session)

	wsConn, _ := session.UpstreamTargetConnection.(*service.WebsocketServiceConn)
	if wsConn != nil {
		log.Infoln("ws backend conn released")
		err = middleware.pool.ReleaseStatefulConn(wsConn)
		if err != nil {
			log.Warnf("release websocket connection error: %s\n", err.Error())
		}
		//err = session.UpstreamTargetConnection.Close()
		//if err == nil {
		//	session.UpstreamTargetConnection = nil
		//}
	}
	go func() {
		select {
		case <- session.ConnectionInitedChan:
			return
		default:
			close(session.ConnectionInitedChan)
		}
	}()
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
	//session.UpstreamRpcRequestsChan <- rpc.NewJSONRpcRequestBundle(messageType, message, rpcRequest, rpcResponseFutureChan)
	wsConn, _ := session.UpstreamTargetConnection.(*service.WebsocketServiceConn)
	connRequestChan := wsConn.RpcRequestChan
	connRequestChan <- rpc.NewJSONRpcRequestBundle(messageType, message, rpcRequest, rpcResponseFutureChan)
}

func (middleware *WsUpstreamMiddleware) OnWebSocketFrame(session *rpc.JSONRpcRequestSession,
	messageType int, message []byte) (err error) {
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
	defer func() {
		if err == nil {
			err = middleware.NextOnJSONRpcRequest(session)
		}
	}()
	connSession := session.Conn

	select {
	case <- connSession.ConnectionInitedChan:
	case <- time.After(5 * time.Second):
		// TODO: send connection error response
		return errors.New("connection timeout")
	}

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

	wsConn, _ := connSession.UpstreamTargetConnection.(*service.WebsocketServiceConn)
	rpcRequest.OriginId = rpcRequest.Id
	rpcRequest.Id = wsConn.NextId()

	session.Conn.SubscribingRequestIds[rpcRequest.Id] = rpcRequest.OriginId // TODO: 修改map的并发问题

	middleware.sendRequestToTargetConn(connSession, websocket.TextMessage, rpcRequestBytes, rpcRequest, session.RpcResponseFutureChan)
	return
}
func (middleware *WsUpstreamMiddleware) OnRpcResponse(session *rpc.JSONRpcRequestSession) (err error) {
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
