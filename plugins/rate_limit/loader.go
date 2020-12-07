package rate_limit

import (
	"github.com/PMLBlockchain/light_node/config"
	"github.com/PMLBlockchain/light_node/plugin"
)

func LoadRateLimitPluginConfig(chain *plugin.MiddlewareChain, configInfo *config.ServerConfig) {
	rateLimiterPluginConf := configInfo.Plugins.RateLimit
	if rateLimiterPluginConf.Start {
		if rateLimiterPluginConf.ConnectionRate <= 0 {
			rateLimiterPluginConf.ConnectionRate = 1000000
		}
		if rateLimiterPluginConf.RpcRate <= 0 {
			rateLimiterPluginConf.RpcRate = 10000000
		}
		rateLimiterMiddleware := NewRateLimiterMiddleware(rateLimiterPluginConf.ConnectionRate, rateLimiterPluginConf.RpcRate)
		chain.InsertHead(rateLimiterMiddleware)
	}
}
