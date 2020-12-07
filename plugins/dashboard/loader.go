package dashboard

import (
	"github.com/PMLBlockchain/light_node/common"
	"github.com/PMLBlockchain/light_node/config"
	"github.com/PMLBlockchain/light_node/plugin"
	"github.com/PMLBlockchain/light_node/plugins/statistic"
	"github.com/PMLBlockchain/light_node/registry"
)

func LoadDashboardPluginConfig(chain *plugin.MiddlewareChain,
	configInfo *config.ServerConfig, r registry.Registry, store statistic.MetricStore) {
	dashboardConfig := configInfo.Plugins.Dashboard
	if !dashboardConfig.Start {
		return
	}
	if len(dashboardConfig.Endpoint) < 1 {
		return
	}
	var registryOption common.Option
	if r != nil {
		registryOption = WithRegistry(r)
	}
	var storeOption common.Option
	if store != nil {
		storeOption = WithStore(store)
	}
	plugin := NewDashboardMiddleware(Endpoint(dashboardConfig.Endpoint), registryOption, storeOption)
	chain.InsertHead(plugin)
}
