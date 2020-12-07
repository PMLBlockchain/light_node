package providers

import (
	"github.com/PMLBlockchain/light_node/rpc"
	"github.com/PMLBlockchain/light_node/utils"
)

var log = utils.GetLogger("provider")

type RpcProviderProcessor interface {
	NotifyNewConnection(connSession *rpc.ConnectionSession) error
	OnConnectionClosed(connSession *rpc.ConnectionSession) error
	OnRawRequestMessage(connSession *rpc.ConnectionSession, rpcSession *rpc.JSONRpcRequestSession,
		messageType int, message []byte) error
	OnRpcRequest(connSession *rpc.ConnectionSession, rpcSession *rpc.JSONRpcRequestSession) error
}

type RpcProvider interface {
	SetRpcProcessor(processor RpcProviderProcessor)
	ListenAndServe() error
}
