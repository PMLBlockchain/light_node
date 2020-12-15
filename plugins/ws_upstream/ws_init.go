package ws_upstream

import (
	"errors"
	"github.com/PMLBlockchain/light_node/service"
	"github.com/gorilla/websocket"
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

	return nil
}
