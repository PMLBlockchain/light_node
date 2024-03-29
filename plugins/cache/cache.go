package cache

import (
	"encoding/json"
	"fmt"
	"github.com/PMLBlockchain/light_node/plugin"
	"github.com/PMLBlockchain/light_node/rpc"
	"github.com/PMLBlockchain/light_node/utils"
	"time"
)

var log = utils.GetLogger("cache")

type CacheConfigItem struct {
	MethodName    string
	CacheDuration time.Duration
}

type CacheMiddleware struct {
	plugin.MiddlewareAdapter
	cacheConfigItems    []*CacheConfigItem
	cacheConfigItemsMap map[string]*CacheConfigItem // methodNameForCache => *CacheConfigItem

	rpcCache *utils.MemoryCache
}

func NewCacheMiddleware(cacheConfigItems ...*CacheConfigItem) *CacheMiddleware {
	cacheConfigItemsMap := make(map[string]*CacheConfigItem)
	result := &CacheMiddleware{
		cacheConfigItems:    nil,
		cacheConfigItemsMap: cacheConfigItemsMap,
		rpcCache:            utils.NewMemoryCache(),
	}
	for _, item := range cacheConfigItems {
		_ = result.AddCacheConfigItem(item)
	}
	return result
}

func (middleware *CacheMiddleware) AddCacheConfigItem(item *CacheConfigItem) *CacheMiddleware {
	middleware.cacheConfigItems = append(middleware.cacheConfigItems, item)
	if item != nil {
		middleware.cacheConfigItemsMap[item.MethodName] = item
	}
	return middleware
}

func (middleware *CacheMiddleware) Name() string {
	return "cache"
}

func (middleware *CacheMiddleware) OnStart() (err error) {
	return middleware.NextOnStart()
}

func (middleware *CacheMiddleware) OnConnection(session *rpc.ConnectionSession) (err error) {
	return middleware.NextOnConnection(session)
}

func (middleware *CacheMiddleware) OnConnectionClosed(session *rpc.ConnectionSession) (err error) {
	return middleware.NextOnConnectionClosed(session)
}

func (middleware *CacheMiddleware) OnWebSocketFrame(session *rpc.JSONRpcRequestSession,
	messageType int, message []byte) error {
	return middleware.NextOnWebSocketFrame(session, messageType, message)
}

func fetchRpcRequestParams(rpcRequest *rpc.JSONRpcRequest, fetchParamsCount int) (result []interface{}) {
	if fetchParamsCount < 1 || rpcRequest.Params == nil {
		return
	}
	paramsArray, ok := rpcRequest.Params.([]interface{})
	if !ok {
		return
	}
	if fetchParamsCount > len(paramsArray) {
		fetchParamsCount = len(paramsArray)
	}
	result = make([]interface{}, fetchParamsCount)
	for i := 0; i < fetchParamsCount; i++ {
		result[i] = paramsArray[i]
	}
	return
}

func (middleware *CacheMiddleware) getCacheConfigItem(session *rpc.JSONRpcRequestSession) (result *CacheConfigItem, ok bool) {
	methodNameForCache := middleware.getMethodNameForCache(session)
	result, ok = middleware.cacheConfigItemsMap[methodNameForCache]
	return
}

func (middleware *CacheMiddleware) getMethodNameForCache(session *rpc.JSONRpcRequestSession) string {
	if session.MethodNameForCache != nil {
		return *session.MethodNameForCache
	}
	return session.Request.Method
}

func (middleware *CacheMiddleware) OnRpcRequest(session *rpc.JSONRpcRequestSession) (err error) {
	next := true
	defer func() {
		if next {
			err = middleware.NextOnJSONRpcRequest(session)
		}
	}()

	methodNameForCache := middleware.getMethodNameForCache(session)
	if _, ok := middleware.getCacheConfigItem(session); !ok {
		return
	}
	cacheKey, err := middleware.cacheKeyForRpcMethod(methodNameForCache, session.Request.Params)
	if err != nil {
		log.Fatalln("cache key for rpc method error", err)
		return
	}
	cached, ok := middleware.rpcCache.Get(cacheKey)
	if !ok {
		return
	}
	cachedItem, ok := cached.(*rpcResponseCacheItem)
	if !ok {
		return
	}
	// need replace cachedItem's rpc request id
	newRes, err := rpc.CloneJSONRpcResponse(cachedItem.response)
	if err != nil {
		return
	}
	newRes.Id = session.Request.Id
	newResBytes, err := json.Marshal(newRes)
	if err != nil {
		return
	}
	session.Response = newRes
	session.RequestBytes = newResBytes
	session.ResponseSetByCache = true
	next = false
	log.Debugf("rpc method-for-cache %s hit cache", methodNameForCache)
	return
}

// cache by methodName + allRpcParams
func (middleware *CacheMiddleware) cacheKeyForRpcMethod(rpcMethodName string, rpcParams interface{}) (result string, err error) {
	rpcParamsBytes, err := json.Marshal(rpcParams)
	if err != nil {
		return
	}
	result = fmt.Sprintf("cache_rpc_%s$%s", rpcMethodName, string(rpcParamsBytes))
	return
}

type rpcResponseCacheItem struct {
	response      *rpc.JSONRpcResponse
	responseBytes []byte
}

func (middleware *CacheMiddleware) OnRpcResponse(session *rpc.JSONRpcRequestSession) (err error) {
	if session.ResponseSetByCache {
		return // can't update cache time by cached response
	}
	methodNameForCache := middleware.getMethodNameForCache(session)
	cacheConfigItem, ok := middleware.getCacheConfigItem(session)
	if !ok {
		return
	}
	rpcRes := session.Response
	rpcResBytes := session.RequestBytes
	cacheKey, err := middleware.cacheKeyForRpcMethod(methodNameForCache, session.Request.Params)
	if err != nil {
		return
	}
	middleware.rpcCache.Set(cacheKey, &rpcResponseCacheItem{
		response:      rpcRes,
		responseBytes: rpcResBytes,
	}, cacheConfigItem.CacheDuration)
	log.Debugf("rpc method-for-cache %s cached\n", methodNameForCache)

	err = middleware.NextOnJSONRpcResponse(session)
	return
}

func (middleware *CacheMiddleware) ProcessRpcRequest(session *rpc.JSONRpcRequestSession) (err error) {
	if session.ResponseSetByCache {
		return
	}
	err = middleware.NextProcessJSONRpcRequest(session)
	return
}
