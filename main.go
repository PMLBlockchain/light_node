package main

import (
	"flag"
	"github.com/PMLBlockchain/light_node/loader"
	"github.com/PMLBlockchain/light_node/proxy"
	"github.com/PMLBlockchain/light_node/utils"
)

func main() {
	utils.Init()
	var log = utils.GetLogger("main")

	configPath := flag.String("config", "server.json", "configuration file path(default server.json)")
	flag.Parse()

	configInfo, err := loader.LoadConfigFromConfigJsonFile(*configPath)
	if err != nil {
		panic(err)
	}
	err = loader.LoadConsulConfig(configInfo)
	if err != nil {
		panic(err)
	}

	loader.SetLoggerFromConfig(configInfo)
	provider := loader.LoadProviderFromConfig(configInfo)
	server := proxy.NewProxyServer(provider)
	loader.LoadPluginsFromConfig(server, configInfo)

	err = server.StartMiddlewares()
	if err != nil {
		log.Panic("start middlewares error", err.Error())
		return
	}
	log.Printf("loaded middlewares are(count %d):\n", len(server.MiddlewareChain.Middlewares))
	for _, middleware := range server.MiddlewareChain.Middlewares {
		log.Printf("\t- middleware %s\n", middleware.Name())
	}
	server.Start()
}
