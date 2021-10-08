package connection

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"sync"
)

var m = clientManager{
	clientFactory: make(map[string]ClientFactoryFunc),
	lock:          sync.Mutex{},
	clientMap:     make(map[string]*clientWrapper),
}

type clientManager struct {
	lock          sync.Mutex
	clientFactory map[string]ClientFactoryFunc
	clientMap     map[string]*clientWrapper
}

func registerClientFactory(clientType string, creatorFunc ClientFactoryFunc) {
	m.lock.Lock()
	m.clientFactory[clientType] = creatorFunc
	m.lock.Unlock()
}

func GetConnection(connectSelector string) (interface{}, error) {

	m.lock.Lock()
	defer m.lock.Unlock()

	var cliWpr *clientWrapper
	var found bool

	cliWpr, found = m.clientMap[connectSelector]
	if found {
		cliWpr.addRef()
	} else {
		selectCfg := &ConSelector{
			ConnSelectorCfg: connectSelector,
		}
		err := selectCfg.Init()
		if err != nil {
			conf.Log.Errorf("connection selector: %s have error %s.", connectSelector, err)
			return nil, err
		}

		clientCreator, ok := m.clientFactory[selectCfg.Type]
		if !ok {
			conf.Log.Errorf("can not find clientCreator for connection selector : %s only support %s", connectSelector, selectCfg.SupportedType)
			return nil, fmt.Errorf("can not find clientCreator for connection selector : %s. only support %s", connectSelector, selectCfg.SupportedType)
		}

		client := clientCreator(selectCfg)

		cliWpr, err = NewClientWrapper(client, selectCfg)
		if err != nil {
			conf.Log.Errorf("can not create client for connection selector : %s have error %s", connectSelector, err)
			return nil, err
		}

		m.clientMap[connectSelector] = cliWpr
	}

	conf.Log.Infof("connection selector: %s GetConnection count %d.", connectSelector, cliWpr.refCnt)

	return cliWpr.getInstance(), nil
}

func ReleaseConnection(connectSelector string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if v, ok := m.clientMap[connectSelector]; ok {
		v.subRef()
		conf.Log.Infof("connection selector: %s ReleaseConnection count %d.", connectSelector, v.refCnt)
		if v.IsRefEmpty() {
			v.clean()
			delete(m.clientMap, connectSelector)
		}
	}
}
