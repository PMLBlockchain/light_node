package loader

import (
	"context"
	"github.com/PMLBlockchain/light_node/config"
	"github.com/PMLBlockchain/light_node/plugins/cache"
	"github.com/PMLBlockchain/light_node/plugins/dashboard"
	"github.com/PMLBlockchain/light_node/plugins/disable"
	"github.com/PMLBlockchain/light_node/plugins/http_upstream"
	"github.com/PMLBlockchain/light_node/plugins/load_balancer"
	"github.com/PMLBlockchain/light_node/plugins/rate_limit"
	"github.com/PMLBlockchain/light_node/plugins/statistic"
	"github.com/PMLBlockchain/light_node/plugins/ws_upstream"
	"github.com/PMLBlockchain/light_node/providers"
	"github.com/PMLBlockchain/light_node/proxy"
	"github.com/PMLBlockchain/light_node/registry/redis"
	"github.com/PMLBlockchain/light_node/utils"
	"io/ioutil"
	"net/url"
	"strconv"
	"time"
)

var log = utils.GetLogger("loader")

func LoadConfigFromConfigJsonFile(configFilePath string) (configInfo *config.ServerConfig, err error) {
	configFileBytes, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		return
	}
	configInfo = new(config.ServerConfig)
	err = config.UnmarshalServerConfigFromJson(configFileBytes, configInfo)
	if err != nil {
		return
	}

	return
}

// 主动上报具体的服务状态给consul
func uploadConsulServiceHealthCheck(consulConfig *config.ConsulConfig, wholeConfig *config.ServerConfig) (err error) {
	err = utils.ConsulSubmitHealthChecker(consulConfig)
	return
}

func LoadConsulConfig(configInfo *config.ServerConfig) (err error) {
	if configInfo.Resolver != nil && configInfo.Resolver.Start {
		consulResolver := configInfo.Resolver
		configFileResolver := consulResolver.ConfigFileResolver
		if config.IsConsulResolver(configFileResolver) {
			log.Infof("loading config file from consul ", consulResolver.ConfigFileResolver)
			// 需要尝试从consul kv http api加载配置文件
			var consulErr error
			configPair, consulErr := utils.ConsulGetKV(configFileResolver)
			if consulErr != nil {
				err = consulErr
				return
			}
			configValue := configPair.Value
			log.Infof("config value from consul: %s", configValue)

			newConfigInfo := &config.ServerConfig{}
			err = config.UnmarshalServerConfigFromJson([]byte(configValue), newConfigInfo)
			if err != nil {
				return
			}
			// resolver等属性只从本地读取，所以要修改从网络中获取的配置文件对象
			newConfigInfo.Resolver = configInfo.Resolver
			*configInfo = *newConfigInfo
			log.Infof("loaded new config from consul %s", configValue)
		}
		// 把本服务注册到consul agent
		if len(consulResolver.Endpoint) > 0 {
			err = utils.ConsulRegisterService(consulResolver, configInfo)
			if err != nil {
				return
			}
			// 后台开启心跳服务，自我检测服务状态上报
			healthCheckerIntervalSeconds := consulResolver.HealthCheckIntervalSeconds
			if healthCheckerIntervalSeconds <= 0 {
				healthCheckerIntervalSeconds = 60
			}
			go func() {
				ctx := context.Background()
				// 先立刻上报一次心跳
				err = uploadConsulServiceHealthCheck(consulResolver, configInfo)
				if err != nil {
					log.Errorf("uploadConsulServiceHealthCheck error %s", err.Error())
				}
				// 隔一段时间就发一次心跳包
				for {
					select {
					case <-ctx.Done():
						{
							break
						}
					case <-time.After(time.Duration(healthCheckerIntervalSeconds) * time.Second):
						{
							err = uploadConsulServiceHealthCheck(consulResolver, configInfo)
							if err != nil {
								log.Errorf("uploadConsulServiceHealthCheck error %s", err.Error())
							}
						}
					}
				}
			}()
		}
	}
	return
}

func SetLoggerFromConfig(configInfo *config.ServerConfig) {
	utils.SetLogLevel(configInfo.Log.Level)
	println("logger level set to " + configInfo.Log.Level)
	if len(configInfo.Log.OutputFile) > 0 {
		utils.AddFileOutputToLog(configInfo.Log.OutputFile)
		println("logger file to " + configInfo.Log.OutputFile)
	}
}

func LoadProviderFromConfig(configInfo *config.ServerConfig) providers.RpcProvider {
	addr := configInfo.Endpoint
	log.Info("to start proxy server on " + addr)

	var provider providers.RpcProvider
	switch configInfo.Provider {
	case "http":
		provider = providers.NewHttpJsonRpcProvider(addr, "/", &providers.HttpJsonRpcProviderOptions{
			TimeoutSeconds: 30,
		})
	case "websocket":
		provider = providers.NewWebSocketJsonRpcProvider(addr, "/")
	default:
		provider = providers.NewWebSocketJsonRpcProvider(addr, "/")
	}
	return provider
}

func loadRegistryFromConfig(server *proxy.ProxyServer, configInfo *config.ServerConfig) {
	if !configInfo.Registry.Start || len(configInfo.Registry.Url) < 1 {
		return
	}
	registryUrl := configInfo.Registry.Url
	uri, err := url.ParseRequestURI(registryUrl)
	if err != nil {
		log.Errorf("registry url error %s", err.Error())
		return
	}
	if uri.Scheme == "redis" {
		// redis registry
		redisEndpoint := uri.Host
		p := uri.Path
		redisDb := 0
		var intErr error
		if len(p) > 1 && p[0] == '/' {
			redisDb, intErr = strconv.Atoi(p[1:])
			if intErr != nil {
				log.Errorf("registry url error %s", intErr.Error())
				return
			}
		}

		r := redis.NewRedisRegistry()
		err = r.Init(
			redis.RedisEndpoint(redisEndpoint),
			redis.RedisDatabase(redisDb))
		if err != nil {
			log.Errorf("init redis registry error %s", err.Error())
			return
		}
		server.Registry = r
	} else {
		log.Errorf("not supported registry type %s", uri.Scheme)
	}
}

func LoadPluginsFromConfig(server *proxy.ProxyServer, configInfo *config.ServerConfig) {
	loadRegistryFromConfig(server, configInfo)

	ws_upstream.LoadWsUpstreamPluginConfig(server.MiddlewareChain, configInfo)
	http_upstream.LoadHttpUpstreamPluginConfig(server.MiddlewareChain, configInfo)
	load_balancer.LoadLoadBalancePluginConfig(server.MiddlewareChain, configInfo, server.Registry)
	disable.LoadDisablePluginConfig(server.MiddlewareChain, configInfo)
	cache.LoadCachePluginConfig(server.MiddlewareChain, configInfo)
	cache.LoadBeforeCachePluginConfig(server.MiddlewareChain, configInfo)
	rate_limit.LoadRateLimitPluginConfig(server.MiddlewareChain, configInfo)
	statisticPlugin := statistic.LoadStatisticPluginConfig(server.MiddlewareChain, configInfo, server.Registry)
	var store statistic.MetricStore
	if statisticPlugin != nil {
		store = statisticPlugin.GetStore()
	} else {
		store = statistic.NewDefaultMetricStore()
	}
	dashboard.LoadDashboardPluginConfig(server.MiddlewareChain, configInfo, server.Registry, store)
}
