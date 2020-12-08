light_node
===================

# Usage

* install by go get
```
go get github.com/PMLBlockchain/light_node

# and then cd to the light_node as work dir
go build
```

* copy `server.sample.json` to `server.json` and update the config json file

```
// example of server.json
{
  "resolver": {
      "start": false,
      "id": "light_node_service_1",
      "name": "light_node",
      "endpoint": "consul://127.0.0.1:8500",
      "config_file_resolver": "consul://127.0.0.1:8500/v1/kv/light_node/dev/config",
      "tags": ["light_node", "web", "dev"],
      "health_checker_interval": 30
    },
  "endpoint": "127.0.0.1:5000",
  "provider": "websocket",
  "log": {
    "level": "INFO",
    "output_file": "logs/light_node.log"
  },
  "registry": {
    "start": true,
    "url": "redis://127.0.0.1:6379/1"
  },
  "plugins": {
    "upstream": {
      "upstream_endpoints": [
        {
          "url": "ws://127.0.0.1:3000", "weight": 1
        },
        {
          "url": "wss://127.0.0.1:4000", "weight": 2
        }
      ]
    },
    "caches": [
      { "name": "dummyMethod", "expire_seconds": 5 },
      { "name": "call", "paramsForCache": [2, "getSomeInfoMethod"],  "expire_seconds": 5 }
    ],
    "before_cache_configs": [
      {"method": "call", "fetch_cache_key_from_params_count": 2}
    ],
    "statistic": {
      "start": true,
      "store": {
        "type": "db",
        "dbUrl": "root:123456@tcp(127.0.0.1:3306)/light_node?parseTime=true&loc=Local",
        "dumpIntervalOpened": true
      }
    },
    "disable": {
      "start": true,
      "disabled_rpc_methods": [
        "stop"
      ]
    },
    "rate_limit": {
      "start": true,
      "connection_rate": 10000,
      "rpc_rate": 1000000
    },
    "dashboard": {
      "start": true,
      "endpoint": ":5000"
    }
  }
}

```

* run the light_node server

```
./light_node -config server.json

# sample output maybe looks like:
{"level":"info","module":"main","msg":"to start proxy server on 127.0.0.1:5000","time":""}
{"level":"info","module":"main","msg":"loaded middlewares are(count 5):\n","time":""}
{"level":"info","module":"main","msg":"\t- middleware statistic\n","time":""}
{"level":"info","module":"main","msg":"\t- middleware before_cache\n","time":""}
{"level":"info","module":"main","msg":"\t- middleware cache\n","time":""}
{"level":"info","module":"main","msg":"\t- middleware load_balance\n","time":""}
{"level":"info","module":"main","msg":"\t- middleware upstream\n","time":""}

```