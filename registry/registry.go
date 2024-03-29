package registry

import (
	"github.com/PMLBlockchain/light_node/common"
)

type Registry interface {
	Init(...common.Option) error
	RegisterService(service *Service) error
	DeregisterService(service *Service) error
	ListServices() ([]*Service, error)
	Watch() (*Watcher, error)
	Close() error
	String() string
}
