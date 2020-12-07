package disable

import (
	"github.com/PMLBlockchain/light_node/config"
	"github.com/PMLBlockchain/light_node/plugin"
)

func LoadDisablePluginConfig(chain *plugin.MiddlewareChain, configInfo *config.ServerConfig) {
	disablePluginConf := configInfo.Plugins.Disable
	if disablePluginConf.Start && len(disablePluginConf.DisabledRpcMethods) > 0 {
		disableMiddleware := NewDisableMiddleware()
		for _, item := range disablePluginConf.DisabledRpcMethods {
			disableMiddleware.AddRpcMethodToBlacklist(item)
		}
		chain.InsertHead(disableMiddleware)
	}
}
