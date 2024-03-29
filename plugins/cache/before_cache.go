package cache

import (
	"encoding/json"
	"github.com/PMLBlockchain/light_node/plugin"
	"github.com/PMLBlockchain/light_node/rpc"
)

type BeforeCacheConfigItem struct {
	MethodName                   string
	FetchCacheKeyFromParamsCount int /* eg. when rpc params: [2, "info", "hello"], and FetchCacheKeyFromParamsCount==2, then methodName for cache middleware will be "call$2$\"info\""*/
}

/**
 * a middleware inserted before cache middleware to fetch `methodName for cache` from rpc request
 * eg. when rpc format is {method: "callOrOther", params: ["realMethodName", ...otherArgs]} for some methods
 */
type BeforeCacheMiddleware struct {
	plugin.MiddlewareAdapter
	beforeCacheConfigItems []*BeforeCacheConfigItem
	configsMap             map[string]*BeforeCacheConfigItem
}

func NewBeforeCacheMiddleware() *BeforeCacheMiddleware {
	return &BeforeCacheMiddleware{
		configsMap: make(map[string]*BeforeCacheConfigItem),
	}
}

func (m *BeforeCacheMiddleware) AddConfigItem(item *BeforeCacheConfigItem) *BeforeCacheMiddleware {
	if item == nil || item.FetchCacheKeyFromParamsCount < 1 {
		return m
	}
	m.beforeCacheConfigItems = append(m.beforeCacheConfigItems, item)
	return m
}

func (m *BeforeCacheMiddleware) Build() {
	for _, item := range m.beforeCacheConfigItems {
		m.configsMap[item.MethodName] = item
	}
}

func (middleware *BeforeCacheMiddleware) Name() string {
	return "before_cache"
}

func (middleware *BeforeCacheMiddleware) OnStart() (err error) {
	return
}

func (middleware *BeforeCacheMiddleware) OnConnection(session *rpc.ConnectionSession) (err error) {
	log.Debugf("middleware %s OnConnection called", middleware.Name())
	return middleware.NextOnConnection(session)
}

func (middleware *BeforeCacheMiddleware) OnConnectionClosed(session *rpc.ConnectionSession) (err error) {
	log.Debugf("middleware %s OnConnectionClosed called", middleware.Name())
	return middleware.NextOnConnectionClosed(session)
}

func (middleware *BeforeCacheMiddleware) OnWebSocketFrame(session *rpc.JSONRpcRequestSession,
	messageType int, message []byte) error {
	return middleware.NextOnWebSocketFrame(session, messageType, message)
}

func (middleware *BeforeCacheMiddleware) findBeforeCacheConfigItem(rpcReq *rpc.JSONRpcRequest) (result *BeforeCacheConfigItem, ok bool) {
	methodName := rpcReq.Method
	rpcParams := rpcReq.Params
	rpcParamsArray, parseArrayOk := rpcParams.([]interface{})
	if !parseArrayOk {
		return
	}
	rpcParamsCount := len(rpcParamsArray)
	result, ok = middleware.configsMap[methodName]
	if ok {
		if rpcParamsCount < result.FetchCacheKeyFromParamsCount {
			ok = false
			result = nil
		}
	}
	return
}

func MakeMethodNameForCache(methodName string, paramsArray []interface{}) (result string, err error) {
	result = methodName
	for i := 0; i < len(paramsArray); i++ {
		result += "$"
		argBytes, jsonErr := json.Marshal(paramsArray[i])
		if jsonErr != nil {
			err = jsonErr
			return
		}
		result += string(argBytes)
	}
	return
}

func (middleware *BeforeCacheMiddleware) OnRpcRequest(session *rpc.JSONRpcRequestSession) (err error) {
	log.Debugf("middleware %s OnRpcRequest called", middleware.Name())
	defer func() {
		if err == nil {
			err = middleware.NextOnJSONRpcRequest(session)
		}
	}()
	rpcReq := session.Request
	beforeCacheConfigItem, ok := middleware.findBeforeCacheConfigItem(rpcReq)
	if !ok {
		return
	}
	rpcParams := rpcReq.Params
	rpcParamsArray, parseArrayOk := rpcParams.([]interface{})
	if !parseArrayOk {
		return
	}
	fetchCacheKeyFromParamsCount := beforeCacheConfigItem.FetchCacheKeyFromParamsCount
	methodNameForCache, jsonErr := MakeMethodNameForCache(rpcReq.Method, rpcParamsArray[0:fetchCacheKeyFromParamsCount])
	if jsonErr != nil {
		log.Println("[before-cache] before_cache middleware parse param json error:", jsonErr)
		return
	}
	session.MethodNameForCache = &methodNameForCache

	// log.Debugf("[before-cache] methodNameForCache %s set\n", methodNameForCache)
	return
}
func (middleware *BeforeCacheMiddleware) OnRpcResponse(session *rpc.JSONRpcRequestSession) error {
	log.Debugf("middleware %s OnRpcResponse called", middleware.Name())
	return middleware.NextOnJSONRpcResponse(session)
}

func (middleware *BeforeCacheMiddleware) ProcessRpcRequest(session *rpc.JSONRpcRequestSession) error {
	log.Debugf("middleware %s ProcessRpcRequest called", middleware.Name())
	return middleware.NextProcessJSONRpcRequest(session)
}
