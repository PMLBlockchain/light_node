package ws_upstream

import (
	"encoding/json"
	"errors"
	"github.com/PMLBlockchain/light_node/rpc"
	"github.com/PMLBlockchain/light_node/service"
	"github.com/gorilla/websocket"
	"time"
)

// 和upstream的websocket连接建立后的hook
func WsConnInitCallback (conn service.ServiceConn) error {
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
	wsConn.SetRpcIdGen(6)

	_ = wsConn

	connRequestChan := make(chan *rpc.JSONRpcRequestBundle, 1024)
	connResponseChan := make(chan *rpc.JSONRpcResponse, 1024)

	wsConn.RpcRequestChan = connRequestChan
	wsConn.RpcResponseChan = connResponseChan

	go func() {
		// 读取连接数据放入请求队列，
		defer close(connRequestChan)
		defer close(connResponseChan)
		defer func() {
			// 关闭连接并且在连接池中移除这个连接
			wsConn.Close()
		}()

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
