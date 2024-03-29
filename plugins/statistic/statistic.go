package statistic

/**
 * statistic middleware
 * async calculate statistic metrics and publish to admin users
 */

import (
	"context"
	"github.com/PMLBlockchain/light_node/common"
	"github.com/PMLBlockchain/light_node/plugin"
	"github.com/PMLBlockchain/light_node/registry"
	"github.com/PMLBlockchain/light_node/rpc"
	"github.com/PMLBlockchain/light_node/utils"
	"github.com/sparrc/go-ping"
	"time"
)

var log = utils.GetLogger("statistic")

type StatisticMiddleware struct {
	plugin.MiddlewareAdapter
	rpcRequestsReceived  chan *rpc.JSONRpcRequestSession
	rpcResponsesReceived chan *rpc.JSONRpcRequestSession

	metricOptions *MetricOptions
	store         MetricStore
}

func NewStatisticMiddleware(options ...common.Option) *StatisticMiddleware {
	const maxRpcChannelSize = 10000

	mOptions := &MetricOptions{}
	for _, o := range options {
		o(mOptions)
	}

	var store MetricStore
	if mOptions.store != nil {
		store = mOptions.store
	} else {
		store = NewDefaultMetricStore()
	}

	err := store.Init()
	if err != nil {
		log.Fatalf("statistic store init error %s", err.Error())
	}

	return &StatisticMiddleware{
		rpcRequestsReceived:  make(chan *rpc.JSONRpcRequestSession, maxRpcChannelSize),
		rpcResponsesReceived: make(chan *rpc.JSONRpcRequestSession, maxRpcChannelSize),
		metricOptions:        mOptions,
		store:                store,
	}
}

func (middleware *StatisticMiddleware) Name() string {
	return "statistic"
}

func (m *StatisticMiddleware) GetStore() MetricStore {
	return m.store
}

func getMethodNameForRpcStatistic(session *rpc.JSONRpcRequestSession) string {
	if session.MethodNameForCache != nil {
		return *session.MethodNameForCache // cache name is more acurrate for statistic
	}
	return session.Request.Method
}

func (middleware *StatisticMiddleware) OnStart() (err error) {
	go func() {
		ctx := context.Background()

		store := middleware.store

		dumpIntervalOpened := middleware.metricOptions.dumpIntervalOpened
		dumpTick := time.Tick(60 * time.Second)

		// 从registry监听服务上下限，如果有掉线，记录alert
		r := middleware.metricOptions.r // registry
		var watcher *registry.Watcher
		var registryEventChan chan *registry.Event
		if r != nil {
			watcher, watcherErr := r.Watch()
			if watcherErr != nil {
				log.Error("watch registry error", watcherErr)
				return
			}
			registryEventChan = watcher.C()
		}

		// 定时检测到各upstream的ping连接
		pingTick := time.Tick(5 * time.Minute)

		for {
			select {
			case <-ctx.Done():
				if watcher != nil {
					watcher.Close()
				}
				return
			case <-dumpTick:
				if !dumpIntervalOpened {
					continue
				}
				// notify user every tick time
			case <-pingTick:
				// ping all upstream servers
				if r == nil {
					continue
				}
				allServices, listErr := r.ListServices()
				if listErr != nil {
					continue
				}
				for _, s := range allServices {
					if len(s.Host) < 1 {
						continue
					}
					go func(s *registry.Service) {
						host := s.Host
						log.Infof("start ping %s", host)
						pinger, pingErr := ping.NewPinger(host)
						if pingErr != nil {
							return
						}
						pinger.Count = 3
						pinger.SetPrivileged(true)
						pinger.Run() // blocks until finished
						stats := pinger.Statistics()
						connected := true
						if stats.PacketsRecv < 1 {
							connected = false
							log.Errorf("service host %s ping error", host)
						}
						rtt := stats.AvgRtt
						log.Infof("service host %s avg RTT %d ms", host, rtt.Nanoseconds()/1e6)
						// update to store
						store.UpdateServiceHostPing(context.Background(), s, rtt, connected)
					}(s)
				}

			case reqSession := <-middleware.rpcRequestsReceived:
				methodNameForStatistic := getMethodNameForRpcStatistic(reqSession)

				store.addRpcMethodCall(methodNameForStatistic)

				// TODO: 根据策略随机采样或者全部记录请求和返回的数据
				includeDebug := true
				store.LogRequest(ctx, reqSession, includeDebug)
			case resSession := <-middleware.rpcResponsesReceived:
				includeDebug := true
				store.logResponse(ctx, resSession, includeDebug)
			case registryEvent := <-registryEventChan:
				log.Infof("receive registry event %s", registryEvent.String())
				// 如果是服务掉线，发出提醒记录到数据库
				switch registryEvent.Type {
				case registry.SERVICE_REMOVE:
					store.LogServiceDown(ctx, registryEvent.ServiceInfo)
				}
			default:
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()
	return middleware.NextOnStart()
}

func (middleware *StatisticMiddleware) OnConnection(session *rpc.ConnectionSession) (err error) {
	return middleware.NextOnConnection(session)
}

func (middleware *StatisticMiddleware) OnConnectionClosed(session *rpc.ConnectionSession) (err error) {
	return middleware.NextOnConnectionClosed(session)
}

func (middleware *StatisticMiddleware) OnWebSocketFrame(session *rpc.JSONRpcRequestSession,
	messageType int, message []byte) (err error) {
	return middleware.NextOnWebSocketFrame(session, messageType, message)
}
func (middleware *StatisticMiddleware) OnRpcRequest(session *rpc.JSONRpcRequestSession) (err error) {
	middleware.rpcRequestsReceived <- session
	return middleware.NextOnJSONRpcRequest(session)
}
func (middleware *StatisticMiddleware) OnRpcResponse(session *rpc.JSONRpcRequestSession) (err error) {
	err = middleware.NextOnJSONRpcResponse(session)
	middleware.rpcResponsesReceived <- session
	return
}

func (middleware *StatisticMiddleware) ProcessRpcRequest(session *rpc.JSONRpcRequestSession) (err error) {
	return middleware.NextProcessJSONRpcRequest(session)
}
