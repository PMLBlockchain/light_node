package providers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/PMLBlockchain/light_node/hx_sdk"
	"github.com/PMLBlockchain/light_node/hx_sdk/hx"
	"github.com/PMLBlockchain/light_node/rpc"
	"github.com/PMLBlockchain/light_node/utils"
	"io/ioutil"
	"net/http"
	"reflect"
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

	databaseApis  map[string]bool
	historyApis   map[string]bool
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

		databaseApis:  databaseApis,
		historyApis:   historyApis,
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

func (provider *HttpJsonRpcProvider) processRpcRequest(ctx context.Context,
	connSession *rpc.ConnectionSession, w http.ResponseWriter, r *http.Request, rpcSession *rpc.JSONRpcRequestSession) (err error) {
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
		rpcReq.Params = []interface{}{2, rpcMethod, rpcParams}
		return rpcReq, nil
	}
	// TODO: history_api接口的拦截
	// TODO: broadcast_api接口拦截

	switch rpcMethod {
	case "hello": {
		// TODO: 这里一个直接处理RPC的例子
		rpcRes := rpc.NewJSONRpcResponse(rpcReq.Id, "this is hello world response", nil)
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
	case "transfer_to_address":
		{
			//url := provider.endpoint

			res := utils.QuerySelf("http://"+provider.endpoint, "{\"method\":\"call\",\"params\":[5,\"get_info\", []],\"id\":10}")
			data := make(map[string]interface{})
			err := json.Unmarshal([]byte(res), &data)
			if err != nil {
				return nil, err
			}
			fmt.Println(data)
			res = utils.QuerySelf("http://"+provider.endpoint, fmt.Sprintf("{\"method\":\"call\",\"params\":[2,\"get_block_header\", [%d]],\"id\":10}", int(data["result"].(map[string]interface{})["current_block_height"].(float64))))
			fmt.Println(res)
			data = make(map[string]interface{})
			err = json.Unmarshal([]byte(res), &data)
			if err != nil {
				return nil, err
			}
			ref_info := hx_sdk.CalRefInfo(data["result"].(map[string]interface{})["previous"].(string))
			trx_data, err := hx_sdk.HxTransfer(ref_info, "5K9BGZwoHwMCEywzFHSMz8T1nJzaK5v7ojzYHEiZyviXG2LS1ox", "ea1ecf2d8a22d5894280aca2327423f42226e0ecf656f4869972c1c83b6f2a63", "HXNQgjSoqZZsLLRLaHX1sQV1KJtV1j3L72uz", "HXNigr6YJH5M8QaMQ4bFjBuFvNDnHdYAbAUG", "HX", "0.1", "0.00101", "biyong test 001", "")
			if err != nil {
				return nil, err
			}
			rpcReq.Method = "call"
			json_data := hx.Transaction{}
			err = json.Unmarshal(trx_data, &json_data)
			if err != nil {
				return nil, err
			}
			fmt.Println(string(trx_data))
			fmt.Println(json_data)
			rpcReq.Params = []interface{}{3, "broadcast_transaction_synchronous", []interface{}{json_data}}
			fmt.Println(rpcReq.Params)
			return rpcReq, nil

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
	// 先解析请求，如果请求能处理就直接处理
	rpcSession, finish, err := provider.receiveConnectionMessage(ctx, connSession, w, r)
	if err != nil {
		sendErrorResponse(w, err, rpc.RPC_INTERNAL_ERROR, 0)
		return
	}
	if finish {
		// 如果已经处理完成了，直接返回
		return
	}

	// 通知backend有新连接
	defer provider.rpcProcessor.OnConnectionClosed(connSession)
	if connErr := provider.rpcProcessor.NotifyNewConnection(connSession); connErr != nil {
		log.Warn("OnConnection error", connErr)
		return
	}

	// 处理解析到的的RPC请求，通知backend去处理
	err = provider.processRpcRequest(ctx, connSession, w, r, rpcSession)
	if err != nil {
		sendErrorResponse(w, err, rpc.RPC_INTERNAL_ERROR, 0)
		return
	}

	// 等待收到backend处理完成的RPC返回
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
