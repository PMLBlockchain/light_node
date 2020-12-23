package main

import (
	"flag"
	"fmt"
	"github.com/PMLBlockchain/light_node/global"
	"github.com/PMLBlockchain/light_node/hx_sdk"
	"github.com/PMLBlockchain/light_node/loader"
	"github.com/PMLBlockchain/light_node/proxy"
	"github.com/PMLBlockchain/light_node/utils"
)

func main() {

	nodeWif, nodePub, addr, err := hx_sdk.GetNewPrivate()
	fmt.Println(nodeWif, nodePub, addr, err)
	utils.Init()
	var log = utils.GetLogger("main")

	configPath := flag.String("config", "server.json", "configuration file path(default server.json)")
	walletPath := flag.String("wallet-json", "wallet.json", "wallet json file path(default wallet.json)")

	flag.Parse()

	configInfo, err := loader.LoadConfigFromConfigJsonFile(*configPath)
	if err != nil {
		panic(err)
	}
	err = loader.LoadConsulConfig(configInfo)
	if err != nil {
		panic(err)
	}

	global.GlobalServerConfig, err = global.LoadOrCreateWalletFile(*walletPath)
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
