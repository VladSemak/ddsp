package frontend

import (
	"sync"
	"time"

	rclient "router/client"
	"router/router"
	"storage"
)

// InitTimeout is a timeout to wait after unsuccessful List() request to Router.
//
// InitTimeout -- количество времени, которое нужно подождать до следующей попытки
// отправки запроса List() в Router.
const InitTimeout = 100 * time.Millisecond

// Config stores configuration for a Frontend service.
//
// Config -- содержит конфигурацию Frontend.
type Config struct {
	// Addr is an address to listen at.
	// Addr -- слушающий адрес Frontend.
	Addr storage.ServiceAddr
	// Router is an address of Router service.
	// Router -- адрес Router service.
	Router storage.ServiceAddr

	// NC specifies client for Node.
	// NC -- клиент для node.
	NC storage.Client `yaml:"-"`
	// RC specifies client for Router.
	// RC -- клиент для router.
	RC rclient.Client `yaml:"-"`
	// NodesFinder specifies a NodeFinder to use.
	// NodesFinder -- NodesFinder, который нужно использовать в Frontend.
	NF router.NodesFinder `yaml:"-"`
}

// Frontend is a frontend service.
type Frontend struct {
	conf Config
	nodes []storage.ServiceAddr
	once sync.Once
}

// New creates a new Frontend with a given cfg.
//
// New создает новый Frontend с данным cfg.
func New(cfg Config) *Frontend {
	return &Frontend{conf: cfg, nodes: nil}
}

func (fe *Frontend) putDel(k storage.RecordID, action func(node storage.ServiceAddr) error) error {
	nodes, err := fe.conf.RC.NodesFind(fe.conf.Router, k)
	if err != nil {
		return err
	}
	
	if len(nodes) < storage.MinRedundancy {
		return storage.ErrNotEnoughDaemons
	}

	errs := make(chan error, len(nodes))
	for _, node := range nodes {
		go func(node storage.ServiceAddr) {
			errs <- action(node)
		}(node)
	}

	errMap := make(map[error]int)
	for range nodes {
		err := <- errs
		if err != nil {
			errMap[err]++
		}
	}


	for err, nerr := range errMap {
		if nerr >= storage.MinRedundancy {
			return err
		}
	}
	count := len(nodes) - len(errMap)
	if count >= storage.MinRedundancy {
		return nil
	}

	return storage.ErrQuorumNotReached
}
// Put an item to the storage if an item for the given key doesn't exist.
// Returns error otherwise.
//
// Put -- добавить запись в хранилище, если запись для данного ключа
// не существует. Иначе вернуть ошибку.
func (fe *Frontend) Put(k storage.RecordID, d []byte) error {
	return fe.putDel(k, func(node storage.ServiceAddr) error {
		return fe.conf.NC.Put(node, k, d)
	})
}

// Del an item from the storage if an item exists for the given key.
// Returns error otherwise.
//
// Del -- удалить запись из хранилища, если запись для данного ключа
// существует. Иначе вернуть ошибку.
func (fe *Frontend) Del(k storage.RecordID) error {
	return fe.putDel(k, func(node storage.ServiceAddr) error {
		return fe.conf.NC.Del(node, k)
	})
}

// Get an item from the storage if an item exists for the given key.
// Returns error otherwise.
//
// Get -- получить запись из хранилища, если запись для данного ключа
// существует. Иначе вернуть ошибку.
func (fe *Frontend) Get(k storage.RecordID) ([]byte, error) {
	fe.once.Do(func(){
		for {
			var err error
			fe.nodes, err = fe.conf.RC.List(fe.conf.Router)
			if err == nil {
				break
			}
			time.Sleep(InitTimeout)
		}
	})

	nodes := fe.conf.NF.NodesFind(k, fe.nodes)
	if len(nodes) < storage.MinRedundancy {
		return nil, storage.ErrNotEnoughDaemons
	}

	type result struct {
		data []byte
		err error
	}

	dataCh := make(chan result, len(nodes))

	for _, node := range nodes {
		go func(node storage.ServiceAddr) {
			data, err := fe.conf.NC.Get(node, k)
			dataCh <- result{data, err}
		}(node)
	}

	data := make(map[string]int)
	errs := make(map[error]int)
	for range nodes {
		res := <-dataCh

		if res.err == nil {
			data[string(res.data)]++
			if data[string(res.data)] >= storage.MinRedundancy {
				return res.data, nil
			}
			continue
		}

		errs[res.err]++
		if errs[res.err] >= storage.MinRedundancy {
			return nil, res.err
		}
	}

	return nil, storage.ErrQuorumNotReached
}
