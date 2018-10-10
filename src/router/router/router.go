package router

import (
	"sync"
	"time"

	"storage"
)

// Config stores configuration for a Router service.
//
// Config -- содержит конфигурацию Router.
type Config struct {
	// Addr is an address to listen at.
	// Addr -- слушающий адрес.
	Addr storage.ServiceAddr

	// Nodes is a list of nodes served by the Router.
	// Nodes -- список node обслуживаемых Router.
	Nodes []storage.ServiceAddr

	// ForgetTimeout is a timeout after node is considered to be unavailable
	// in absence of hearbeats.
	// ForgetTimeout -- если в течении ForgetTimeout node не посылала heartbeats, то
	// node считается недоступной.
	ForgetTimeout time.Duration `yaml:"forget_timeout"`

	// NodesFinder specifies a NodesFinder to use.
	// NodesFinder -- NodesFinder, который нужно использовать в Router.
	NodesFinder NodesFinder `yaml:"-"`
}

// Router is a router service.
type Router struct {
	conf Config
	hb   map[storage.ServiceAddr]time.Time
	lock sync.RWMutex
}

// New creates a new Router with a given cfg.
// Returns storage.ErrNotEnoughDaemons error if less then storage.ReplicationFactor
// nodes was provided in cfg.Nodes.
//
// New создает новый Router с данным cfg.
// Возвращает ошибку storage.ErrNotEnoughDaemons если в cfg.Nodes
// меньше чем storage.ReplicationFactor nodes.
func New(cfg Config) (*Router, error) {
	if len(cfg.Nodes) < storage.ReplicationFactor {
		return nil, storage.ErrNotEnoughDaemons
	}

	hb := make(map[storage.ServiceAddr]time.Time, len(cfg.Nodes))

	for _, node := range cfg.Nodes {
		hb[node] = time.Now()
	}
	return &Router{conf: cfg, hb: hb}, nil
}

// Hearbeat registers node in the router.
// Returns storage.ErrUnknownDaemon error if node is not served by the Router.

// Hearbeat регистритрует node в router.
// Возвращает ошибку storage.ErrUnknownDaemon если node не
// обслуживается Router.
func (r *Router) Heartbeat(node storage.ServiceAddr) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if _, ok := r.hb[node]; ok {
		r.hb[node] = time.Now()
		return nil
	}
	return storage.ErrUnknownDaemon
}

// NodesFind returns a list of available nodes, where record with associated key k
// should be stored. Returns storage.ErrNotEnoughDaemons error
// if less then storage.MinRedundancy can be returned.
//
// NodesFind возвращает cписок достпуных node, на которых должна храниться
// запись с ключом k. Возвращает ошибку storage.ErrNotEnoughDaemons
// если меньше, чем storage.MinRedundancy найдено.
func (r *Router) NodesFind(k storage.RecordID) ([]storage.ServiceAddr, error) {
	posibleNodes := r.conf.NodesFinder.NodesFind(k, r.conf.Nodes)

	availNodes := make([]storage.ServiceAddr, 0, len(posibleNodes))

	for _, node := range posibleNodes {
		r.lock.RLock()

		diff := time.Now().Sub(r.hb[node])
		if diff < r.conf.ForgetTimeout {
			availNodes = append(availNodes, node)
		}

		r.lock.RUnlock()
	}

	if len(availNodes) < storage.MinRedundancy {
		return nil, storage.ErrNotEnoughDaemons
	}
	return availNodes, nil
}

// List returns a list of all nodes served by Router.
//
// List возвращает cписок всех node, обслуживаемых Router.
func (r *Router) List() []storage.ServiceAddr {
	return r.conf.Nodes
}
