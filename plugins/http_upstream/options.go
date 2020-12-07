package http_upstream

import (
	"github.com/PMLBlockchain/light_node/common"
	"time"
)

type httpUpstreamMiddlewareOptions struct {
	defaultTargetEndpoint string
	upstreamTimeout       time.Duration
}

func HttpDefaultTargetEndpoint(endpoint string) common.Option {
	return func(options common.Options) {
		mOptions := options.(*httpUpstreamMiddlewareOptions)
		mOptions.defaultTargetEndpoint = endpoint
	}
}

func HttpUpstreamTimeout(timeout time.Duration) common.Option {
	return func(options common.Options) {
		mOptions := options.(*httpUpstreamMiddlewareOptions)
		mOptions.upstreamTimeout = timeout
	}
}
