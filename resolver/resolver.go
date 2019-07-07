package resolver

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc/resolver"
)

// GrpcResolverBuilder ...
type GrpcResolverBuilder struct {
	client *clientv3.Client
}

// Build ...
func (b *GrpcResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOption) (resolver.Resolver, error) {
	r := &GrpcResolver{
		client:   b.client,
		cc:       cc,
		endpoint: target.Endpoint,
	}

	if err := r.start(); err != nil {
		log.Println(err.Error())
		return nil, err
	}

	go r.watch()
	return r, nil
}

// Scheme ...
func (*GrpcResolverBuilder) Scheme() string {
	return "discovery"
}

// GrpcResolver ...
type GrpcResolver struct {
	client   *clientv3.Client
	cc       resolver.ClientConn
	endpoint string
	wch      clientv3.WatchChan
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

func (gr *GrpcResolver) start() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	resp, err := gr.client.Get(ctx, gr.endpoint+"/", clientv3.WithPrefix(), clientv3.WithSerializable())
	if err != nil {
		log.Printf("start get from etcd err: %s\n", err.Error())
		return err
	}

	addrs := make([]resolver.Address, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var jupdate Update
		if err := json.Unmarshal(kv.Value, &jupdate); err != nil {
			log.Printf("unmarshal update err: %s\n", err.Error())
			continue
		}
		addrs = append(addrs, resolver.Address{
			Addr:     jupdate.Addr,
			Metadata: jupdate.Metadata,
		})
	}

	gr.cc.UpdateState(resolver.State{
		Addresses: addrs,
	})

	opts := []clientv3.OpOption{clientv3.WithRev(resp.Header.Revision + 1), clientv3.WithPrefix(), clientv3.WithPrevKV()}
	gr.wch = gr.client.Watch(context.Background(), gr.endpoint+"/", opts...)
	return nil
}

func (gr *GrpcResolver) watch() {
	for {
		wr, ok := <-gr.wch
		if !ok {
			continue
		}

		addresses := make([]resolver.Address, 0, len(wr.Events))
		for _, e := range wr.Events {
			var jupdate Update
			var err error
			switch e.Type {
			case clientv3.EventTypePut:
				err = json.Unmarshal(e.Kv.Value, &jupdate)
			case clientv3.EventTypeDelete:
				err = json.Unmarshal(e.PrevKv.Value, &jupdate)
			default:
				continue
			}
			if err == nil {
				addresses = append(addresses, resolver.Address{
					Addr:     jupdate.Addr,
					Metadata: jupdate.Metadata,
				})
			}
		}
	}
}

// Close ...
func (*GrpcResolver) Close() {}

// ResolveNow ...
func (*GrpcResolver) ResolveNow(o resolver.ResolveNowOption) {}
