package ws_upstream

import (
	"github.com/PMLBlockchain/light_node/common"
	"time"
)

type wsUpstreamMiddlewareOptions struct {
	defaultTargetEndpoint string
	upstreamTimeout       time.Duration
}

func WsDefaultTargetEndpoint(endpoint string) common.Option {
	return func(options common.Options) {
		mOptions := options.(*wsUpstreamMiddlewareOptions)
		mOptions.defaultTargetEndpoint = endpoint
	}
}

func WsUpstreamTimeout(timeout time.Duration) common.Option {
	return func(options common.Options) {
		mOptions := options.(*wsUpstreamMiddlewareOptions)
		mOptions.upstreamTimeout = timeout
	}
}
