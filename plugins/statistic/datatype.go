package statistic

import "github.com/PMLBlockchain/light_node/registry"

type MethodCallCacheInfo struct {
	Expiration int64 `json:"expiration"` // expiration unix nano timestamp, 0 means no expire
	CallCount  int64 `json:"callCount"`
}

type StatData struct {
	GlobalStat         map[string]*MethodCallCacheInfo `json:"globalStat"`
	HourlyStat         map[string]*MethodCallCacheInfo `json:"hourlyStat"`
	GlobalRpcCallCount uint64                          `json:"globalRpcCallCount"`
	HourlyRpcCallCount uint64                          `json:"hourlyRpcCallCount"`

	UpstreamServices []*registry.Service `json:"upstreamServices"`
	Services         []*registry.Service `json:"services"`
}

func NewStatData() *StatData {
	return &StatData{
		GlobalStat:         make(map[string]*MethodCallCacheInfo),
		HourlyStat:         make(map[string]*MethodCallCacheInfo),
		GlobalRpcCallCount: 0,
		HourlyRpcCallCount: 0,
		UpstreamServices:   make([]*registry.Service, 0),
		Services:           make([]*registry.Service, 0),
	}
}
