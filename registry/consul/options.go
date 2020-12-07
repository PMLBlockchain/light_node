package consul

import "github.com/PMLBlockchain/light_node/common"

type consulRegistryOptions struct {
	Endpoint string
}

func newConsulRegistryOptions() *consulRegistryOptions {
	return &consulRegistryOptions{}
}

func ConsulEndpoint(endpoint string) common.Option {
	return func(options common.Options) {
		mOptions := options.(*consulRegistryOptions)
		mOptions.Endpoint = endpoint
	}
}
