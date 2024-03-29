package http_upstream

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/PMLBlockchain/light_node/common"
	"github.com/PMLBlockchain/light_node/plugin"
	pluginsCommon "github.com/PMLBlockchain/light_node/plugins/common"
	"github.com/PMLBlockchain/light_node/rpc"
	"github.com/PMLBlockchain/light_node/utils"
	"io/ioutil"
	"net/http"
	"time"
)

var log = utils.GetLogger("http_upstream")

type HttpUpstreamMiddleware struct {
	plugin.MiddlewareAdapter

	options *httpUpstreamMiddlewareOptions
}

func NewHttpUpstreamMiddleware(argOptions ...common.Option) (*HttpUpstreamMiddleware, error) {
	mOptions := &httpUpstreamMiddlewareOptions{
		upstreamTimeout:       30 * time.Second,
		defaultTargetEndpoint: "",
	}
	for _, o := range argOptions {
		o(mOptions)
	}
	m := &HttpUpstreamMiddleware{
		MiddlewareAdapter: plugin.MiddlewareAdapter{},
		options:           mOptions,
	}
	return m, nil
}

func (m *HttpUpstreamMiddleware) Name() string {
	return "http-upstream"
}

func (m *HttpUpstreamMiddleware) OnStart() (err error) {
	log.Info("http upstream plugin starting")
	return m.NextOnStart()
}

func (m *HttpUpstreamMiddleware) OnConnection(session *rpc.ConnectionSession) (err error) {
	log.Debugln("http upstream plugin on new connection")
	return m.NextOnConnection(session)
}

func (m *HttpUpstreamMiddleware) OnConnectionClosed(session *rpc.ConnectionSession) (err error) {
	// call next first
	err = m.NextOnConnectionClosed(session)
	log.Debugln("http upstream plugin on connection closed")
	return
}

func (m *HttpUpstreamMiddleware) getTargetEndpoint(session *rpc.ConnectionSession) (target string, err error) {
	return pluginsCommon.GetSelectedUpstreamTargetEndpoint(session, &m.options.defaultTargetEndpoint)
}

func (m *HttpUpstreamMiddleware) OnRpcRequest(session *rpc.JSONRpcRequestSession) (err error) {
	defer func() {
		if err == nil {
			err = m.NextOnJSONRpcRequest(session)
		}
	}()
	targetEndpoint, err := m.getTargetEndpoint(session.Conn)
	if err != nil {
		return
	}
	log.Debugln("http stream receive rpc request for backend " + targetEndpoint)
	session.TargetServer = targetEndpoint
	// create response future before to use in ProcessRpcRequest
	session.RpcResponseFutureChan = make(chan *rpc.JSONRpcResponse, 1)
	rpcRequest := session.Request
	rpcRequestBytes, err := json.Marshal(rpcRequest)
	if err != nil {
		log.Debugln("http rpc request format error", err.Error())
		errResp := rpc.NewJSONRpcResponse(rpcRequest.Id, nil,
			rpc.NewJSONRpcResponseError(rpc.RPC_INTERNAL_ERROR, err.Error(), nil))
		session.RpcResponseFutureChan <- errResp
		return
	}
	log.Debugln("rpc request " + string(rpcRequestBytes))

	httpRpcCall := func() (rpcRes *rpc.JSONRpcResponse, err error) {
		resp, err := http.Post(targetEndpoint, "application/json", bytes.NewReader(rpcRequestBytes))
		if err != nil {
			log.Debugln("http rpc response error", err.Error())
			errResp := rpc.NewJSONRpcResponse(rpcRequest.Id, nil,
				rpc.NewJSONRpcResponseError(rpc.RPC_UPSTREAM_CONNECTION_CLOSED_ERROR, err.Error(), nil))
			session.RpcResponseFutureChan <- errResp
			return
		}
		defer resp.Body.Close()
		respMsg, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return
		}
		log.Debugln("backend rpc response " + string(respMsg))
		rpcRes, err = rpc.DecodeJSONRPCResponse(respMsg)
		if err != nil {
			return
		}
		if rpcRes == nil {
			err = errors.New("invalid jsonrpc response format from http upstream: " + string(respMsg))
			return
		}
		return
	}

	go func() {
		rpcRes, err := httpRpcCall()
		if err != nil {
			log.Debugln("http rpc response error", err.Error())
			errResp := rpc.NewJSONRpcResponse(rpcRequest.Id, nil,
				rpc.NewJSONRpcResponseError(rpc.RPC_UPSTREAM_CONNECTION_CLOSED_ERROR, err.Error(), nil))
			session.RpcResponseFutureChan <- errResp
			return
		}
		session.RpcResponseFutureChan <- rpcRes
	}()

	return
}

func (m *HttpUpstreamMiddleware) OnRpcResponse(session *rpc.JSONRpcRequestSession) (err error) {
	defer func() {
		if err == nil {
			err = m.NextOnJSONRpcResponse(session)
		}
	}()

	return
}

func (m *HttpUpstreamMiddleware) ProcessRpcRequest(session *rpc.JSONRpcRequestSession) (err error) {
	defer func() {
		if err == nil {
			err = m.NextProcessJSONRpcRequest(session)
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
	case <-time.After(m.options.upstreamTimeout):
		rpcRes = rpc.NewJSONRpcResponse(rpcRequestId, nil,
			rpc.NewJSONRpcResponseError(rpc.RPC_UPSTREAM_CONNECTION_CLOSED_ERROR,
				"upstream target connection closed", nil))
	case rpcRes = <-requestChan:
		// do nothing, just receive rpcRes
		if rpcRes == nil {
			rpcRes = rpc.NewJSONRpcResponse(rpcRequestId, nil,
				rpc.NewJSONRpcResponseError(rpc.RPC_UPSTREAM_TIMEOUT_ERROR,
					"upstream target connection closed", nil))
		}
	}
	session.Response = rpcRes
	return
}

func (m *HttpUpstreamMiddleware) OnWebSocketFrame(session *rpc.JSONRpcRequestSession,
	messageType int, message []byte) (err error) {
	defer func() {
		if err == nil {
			err = m.NextOnWebSocketFrame(session, messageType, message)
		}
	}()
	return
}
