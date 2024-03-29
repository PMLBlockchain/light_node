package disable

import (
	"github.com/PMLBlockchain/light_node/plugin"
	"github.com/PMLBlockchain/light_node/rpc"
)

/**
 * DisableMiddleware is a middleware which can disable some jsonrpc methods
 */
type DisableMiddleware struct {
	plugin.MiddlewareAdapter
	next                plugin.Middleware
	rpcMethodsBlacklist map[string]interface{}
}

func NewDisableMiddleware() *DisableMiddleware {
	return &DisableMiddleware{
		rpcMethodsBlacklist: make(map[string]interface{}),
	}
}

func (middleware *DisableMiddleware) AddRpcMethodToBlacklist(methodName string) *DisableMiddleware {
	middleware.rpcMethodsBlacklist[methodName] = true
	return middleware
}

func (middleware *DisableMiddleware) Name() string {
	return "disable"
}

func (middleware *DisableMiddleware) isDisabledRpcMethod(methodName string) bool {
	_, ok := middleware.rpcMethodsBlacklist[methodName]
	return ok
}

func (middleware *DisableMiddleware) OnStart() (err error) {
	return middleware.NextOnStart()
}

func (middleware *DisableMiddleware) OnConnection(session *rpc.ConnectionSession) (err error) {
	return middleware.NextOnConnection(session)
}

func (middleware *DisableMiddleware) OnConnectionClosed(session *rpc.ConnectionSession) (err error) {
	return middleware.NextOnConnectionClosed(session)
}

func (middleware *DisableMiddleware) OnWebSocketFrame(session *rpc.JSONRpcRequestSession,
	messageType int, message []byte) (err error) {
	return middleware.NextOnWebSocketFrame(session, messageType, message)
}
func (middleware *DisableMiddleware) OnRpcRequest(session *rpc.JSONRpcRequestSession) (err error) {
	rpcRequest := session.Request
	if middleware.isDisabledRpcMethod(rpcRequest.Method) {
		response := rpc.NewJSONRpcResponse(rpcRequest.Id, nil, rpc.NewJSONRpcResponseError(rpc.RPC_DISABLED_RPC_METHOD, "disabled rpc method", nil))
		session.FillRpcResponse(response)
		return
	}
	return middleware.NextOnJSONRpcRequest(session)
}
func (middleware *DisableMiddleware) OnRpcResponse(session *rpc.JSONRpcRequestSession) (err error) {
	return middleware.NextOnJSONRpcResponse(session)
}

func (middleware *DisableMiddleware) ProcessRpcRequest(session *rpc.JSONRpcRequestSession) (err error) {
	return middleware.NextProcessJSONRpcRequest(session)
}
