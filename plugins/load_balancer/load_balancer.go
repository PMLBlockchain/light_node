package load_balancer

import (
	"errors"
	"github.com/PMLBlockchain/light_node/plugin"
	"github.com/PMLBlockchain/light_node/rpc"
	"github.com/PMLBlockchain/light_node/utils"
)

var log = utils.GetLogger("load_balancer")

type UpstreamItem struct {
	Id             int64
	TargetEndpoint string
	Weight         int64
}

var upstreamItemIdGen int64 = 0

func NewUpstreamItem(targetEndpoint string, weight int64) *UpstreamItem {
	defer func() {
		upstreamItemIdGen++
	}()
	return &UpstreamItem{
		Id:             upstreamItemIdGen,
		TargetEndpoint: targetEndpoint,
		Weight:         weight,
	}
}

type LoadBalanceMiddleware struct {
	plugin.MiddlewareAdapter
	selector      *WrrSelector
	UpstreamItems []*UpstreamItem
}

func NewLoadBalanceMiddleware() *LoadBalanceMiddleware {
	return &LoadBalanceMiddleware{
		selector: NewWrrSelector(),
	}
}

func (middleware *LoadBalanceMiddleware) AddUpstreamItem(item *UpstreamItem) *LoadBalanceMiddleware {
	middleware.UpstreamItems = append(middleware.UpstreamItems, item)
	middleware.selector.AddNode(item.Weight, item)
	return middleware
}

func (middleware *LoadBalanceMiddleware) Name() string {
	return "load_balance"
}

func (middleware *LoadBalanceMiddleware) selectTargetByWeight() *UpstreamItem {
	selected, err := middleware.selector.Next()
	if err != nil {
		log.Fatalln("load balance selector next error", err)
		return nil
	}
	selectedUpStreamItem, ok := selected.(*UpstreamItem)
	if !ok {
		return nil
	}

	log.Debugf("[load-balancer]select target %s", selectedUpStreamItem.TargetEndpoint)
	return selectedUpStreamItem
}

func (middleware *LoadBalanceMiddleware) OnStart() (err error) {
	return middleware.NextOnStart()
}

func (middleware *LoadBalanceMiddleware) OnConnection(session *rpc.ConnectionSession) (err error) {
	selectedTargetItem := middleware.selectTargetByWeight()
	if selectedTargetItem == nil {
		err = errors.New("can't select one upstream target")
		return
	}
	log.Debugf("selected upstream target item id#%d endpoint: %s\n", selectedTargetItem.Id, selectedTargetItem.TargetEndpoint)
	session.SelectedUpstreamTarget = &selectedTargetItem.TargetEndpoint

	return middleware.NextOnConnection(session)
}

func (middleware *LoadBalanceMiddleware) OnConnectionClosed(session *rpc.ConnectionSession) (err error) {
	return middleware.NextOnConnectionClosed(session)
}

func (middleware *LoadBalanceMiddleware) OnWebSocketFrame(session *rpc.JSONRpcRequestSession,
	messageType int, message []byte) (err error) {
	return middleware.NextOnWebSocketFrame(session, messageType, message)
}
func (middleware *LoadBalanceMiddleware) OnRpcRequest(session *rpc.JSONRpcRequestSession) (err error) {
	return middleware.NextOnJSONRpcRequest(session)
}
func (middleware *LoadBalanceMiddleware) OnRpcResponse(session *rpc.JSONRpcRequestSession) (err error) {
	return middleware.NextOnJSONRpcResponse(session)
}

func (middleware *LoadBalanceMiddleware) ProcessRpcRequest(session *rpc.JSONRpcRequestSession) (err error) {
	return middleware.NextProcessJSONRpcRequest(session)
}
