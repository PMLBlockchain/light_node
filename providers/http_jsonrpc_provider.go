package providers

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/PMLBlockchain/light_node/rpc"
	"io/ioutil"
	"net/http"
	"time"
)

type HttpJsonRpcProviderOptions struct {
	TimeoutSeconds uint32
}

type HttpJsonRpcProvider struct {
	endpoint     string
	path         string
	options      *HttpJsonRpcProviderOptions
	rpcProcessor RpcProviderProcessor

	databaseApis map[string]bool
	historyApis map[string]bool
	broadcastApis map[string]bool
}

func NewHttpJsonRpcProvider(endpoint string, path string, options *HttpJsonRpcProviderOptions) *HttpJsonRpcProvider {
	if options == nil {
		log.Fatalln("null HttpJsonRpcProviderOptions provided")
		return nil
	}

	// TODO: rpc apis
	databaseApisArray := []string{"get_chain_id"}
	historyApisArray := []string{}
	broadcastApisArray := []string{}

	databaseApis := make(map[string]bool)
	historyApis := make(map[string]bool)
	broadcastApis := make(map[string]bool)

	for _, api := range databaseApisArray {
		databaseApis[api] = true
	}
	for _, api := range historyApisArray {
		historyApis[api] = true
	}
	for _, api := range broadcastApisArray {
		broadcastApis[api] = true
	}

	return &HttpJsonRpcProvider{
		endpoint:     endpoint,
		path:         path,
		rpcProcessor: nil,
		options:      options,

		databaseApis: databaseApis,
		historyApis: historyApis,
		broadcastApis: broadcastApis,
	}
}

func (provider *HttpJsonRpcProvider) SetRpcProcessor(processor RpcProviderProcessor) {
	provider.rpcProcessor = processor
}

func sendErrorResponse(w http.ResponseWriter, err error, errCode int, requestId uint64) {
	resErr := rpc.NewJSONRpcResponseError(errCode, err.Error(), nil)
	errRes := rpc.NewJSONRpcResponse(requestId, nil, resErr)
	errResBytes, encodeErr := json.Marshal(errRes)
	if encodeErr != nil {
		_, _ = w.Write(errResBytes)
	}
}

func (provider *HttpJsonRpcProvider) asyncWatchMessagesToConnection(ctx context.Context, connSession *rpc.ConnectionSession,
	w http.ResponseWriter, r *http.Request, done chan struct{}) {
	go func() {
		defer func() {
			done <- struct{}{}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-connSession.ConnectionDone:
				return
			case rpcDispatch := <-connSession.RpcRequestsDispatchChannel:
				if rpcDispatch == nil {
					return
				}
				rpcRequestSession := rpcDispatch.Data
				switch rpcDispatch.Type {
				case rpc.RPC_REQUEST_CHANGE_TYPE_ADD_REQUEST:
					rpcRequest := rpcRequestSession.Request
					rpcRequestId := rpcRequest.Id
					newChan := rpcRequestSession.RpcResponseFutureChan
					if old, ok := connSession.RpcRequestsMap[rpcRequestId]; ok {
						close(old)
					}
					connSession.RpcRequestsMap[rpcRequestId] = newChan
				case rpc.RPC_REQUEST_CHANGE_TYPE_ADD_RESPONSE:
					rpcRequest := rpcRequestSession.Request
					rpcRequestId := rpcRequest.Id
					rpcRequestSession.RpcResponseFutureChan = nil
					if resChan, ok := connSession.RpcRequestsMap[rpcRequestId]; ok {
						close(resChan)
						delete(connSession.RpcRequestsMap, rpcRequestId)
					}
				}
			case pack := <-connSession.RequestConnectionWriteChan:
				if pack == nil {
					return
				}
				_, err := w.Write(pack.Message)
				if err != nil {
					log.Warn("write response error", err)
					return
				}
				return
			case <-time.After(time.Duration(provider.options.TimeoutSeconds) * time.Second):
				sendErrorResponse(w, errors.New("timeout"), rpc.RPC_RESPONSE_TIMEOUT_ERROR, 0)
				return
			}
		}
	}()
}

func (provider *HttpJsonRpcProvider) receiveConnectionMessage(ctx context.Context,
	connSession *rpc.ConnectionSession, w http.ResponseWriter, r *http.Request) (rpcSession *rpc.JSONRpcRequestSession, finish bool, err error) {
	body := r.Body
	defer body.Close()
	message, err := ioutil.ReadAll(body)
	if err != nil {
		return
	}
	log.Debugf("recv: %s\n", message)
	rpcSession = rpc.NewJSONRpcRequestSession(connSession)

	messageType := 0

	err = provider.rpcProcessor.OnRawRequestMessage(connSession, rpcSession, messageType, message)
	if err != nil {
		log.Warn("OnRawRequestMessage error", err)
		return
	}
	rpcReq, err := rpc.DecodeJSONRPCRequest(message)
	if err != nil {
		err = errors.New("jsonrpc request error" + err.Error())
		return
	}
	newRpcReq, err := provider.interceptRpcRequest(w, rpcReq)
	if err != nil {
		log.Warnf("interceptRpcRequest error %s\n", err.Error())
		if newRpcReq != nil {
			rpcReq = newRpcReq
		}
	} else {
		if newRpcReq == nil {
			finish = true
			return
		}
	}
	rpcSession.FillRpcRequest(rpcReq, message)
	return
}

func (provider *HttpJsonRpcProvider) watchConnectionMessages(ctx context.Context,
	connSession *rpc.ConnectionSession, w http.ResponseWriter, r *http.Request, rpcSession *rpc.JSONRpcRequestSession) (finish bool, err error) {
	err = provider.rpcProcessor.OnRpcRequest(connSession, rpcSession)
	return
}

func (provider *HttpJsonRpcProvider) interceptRpcRequest(w http.ResponseWriter, rpcReq *rpc.JSONRpcRequest) (*rpc.JSONRpcRequest, error) {
	// 拦截RPC请求，做些处理
	rpcMethod := rpcReq.Method
	rpcParams := rpcReq.Params

	// database_api, history_api, broadcast_api等接口的拦截处理
	if _, ok := provider.databaseApis[rpcMethod]; ok {
		rpcReq.Method = "call"
		rpcReq.Params = []interface{} {2, rpcMethod, rpcParams}
		return rpcReq, nil
	}
	// TODO: history_api接口的拦截
	// TODO: broadcast_api接口拦截

	switch rpcMethod {
	case "hello": {
		// TODO: 这里一个直接处理RPC的例子
		rpcRes := &rpc.JSONRpcResponse{
			Id: rpcReq.Id,
			JSONRpc: "2.0",
			Result: "this is hello world response",
		}
		resResBytes, jsonErr := json.Marshal(rpcRes)
		if jsonErr != nil {
			return nil, jsonErr
		}
		_, err := w.Write(resResBytes)
		if err != nil {
			return nil, err
		}
		return nil, err
	}
	default:

	}

	return rpcReq, nil
}

func (provider *HttpJsonRpcProvider) serverHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		_, _ = w.Write([]byte("ok"))
		return
	}
	if r.Method != http.MethodPost {
		resErr := rpc.NewJSONRpcResponseError(rpc.RPC_INTERNAL_ERROR, "only support POST method", nil)
		errRes := rpc.NewJSONRpcResponse(0, nil, resErr)
		errResBytes, encodeErr := json.Marshal(errRes)
		if encodeErr != nil {
			_, _ = w.Write(errResBytes)
		}
		return
	}
	connSession := rpc.NewConnectionSession()
	defer connSession.Close()

	ctx := context.Background()
	rpcSession, finish, err := provider.receiveConnectionMessage(ctx, connSession, w, r)
	if err != nil {
		sendErrorResponse(w, err, rpc.RPC_INTERNAL_ERROR, 0)
		return
	}
	if finish {
		return
	}

	defer provider.rpcProcessor.OnConnectionClosed(connSession)
	if connErr := provider.rpcProcessor.NotifyNewConnection(connSession); connErr != nil {
		log.Warn("OnConnection error", connErr)
		return
	}

	finish, err = provider.watchConnectionMessages(ctx, connSession, w, r, rpcSession)
	if err != nil {
		sendErrorResponse(w, err, rpc.RPC_INTERNAL_ERROR, 0)
		return
	}
	if finish {
		return
	}

	done := make(chan struct{})
	provider.asyncWatchMessagesToConnection(ctx, connSession, w, r, done)

	select {
	case <-done:
		break
	}
}

func (provider *HttpJsonRpcProvider) ListenAndServe() (err error) {
	if provider.rpcProcessor == nil {
		err = errors.New("please set provider.rpcProcessor before ListenAndServe")
		return
	}
	wrappedHandler := func(w http.ResponseWriter, r *http.Request) {
		provider.serverHandler(w, r)
	}
	http.HandleFunc(provider.path, wrappedHandler)
	return http.ListenAndServe(provider.endpoint, nil)
}
