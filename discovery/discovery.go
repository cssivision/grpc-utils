package discovery

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
)

// Discovery ...
type Discovery struct {
	client *clientv3.Client
	mutex  sync.RWMutex
	store  map[string]*Registry
}

// Registry ...
type Registry struct {
	Addresses []*Address
	client    *clientv3.Client
}

// Address ...
type Address struct {
	Addr     string
	Metadata interface{}
}

// GetRoutes ...
func (ds *Discovery) GetRoutes(appid string) ([]*Address, error) {
	ds.mutex.RLock()
	r, ok := ds.store[appid]
	ds.mutex.RUnlock()
	if !ok {
		ds.mutex.Lock()
		nr, err := NewRegistry(ds.client, appid)
		if err != nil {
			logrus.Error(err.Error())
		}
		ds.store[appid] = nr
		ds.mutex.Unlock()
		return nr.Addresses, nil
	}

	return r.Addresses, nil
}

// Update ...
type Update struct {
	// address
	Addr string
	// meta data
	Metadata map[string]interface{}
	// op
	Op int
}

// NewRegistry ...
func NewRegistry(client *clientv3.Client, appid string) (*Registry, error) {
	target := appid + "/"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	resp, err := client.Get(ctx, target, clientv3.WithPrefix(), clientv3.WithSerializable())
	if err != nil {
		logrus.Error(err.Error())
		return nil, err
	}
	addrs := make([]*Address, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var jupdate Update
		if err := json.Unmarshal(kv.Value, &jupdate); err != nil {
			logrus.Errorf("unmarshal update err: %s\n", err.Error())
			continue
		}
		addrs = append(addrs, &Address{
			Addr:     jupdate.Addr,
			Metadata: jupdate.Metadata,
		})
	}

	return &Registry{
		client:    client,
		Addresses: addrs,
	}, nil
}
