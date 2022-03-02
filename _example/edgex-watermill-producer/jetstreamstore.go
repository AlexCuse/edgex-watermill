package main

import (
	"encoding/json"
	"fmt"
	"github.com/edgexfoundry/app-functions-sdk-go/v2/pkg/interfaces"
	"github.com/edgexfoundry/go-mod-bootstrap/v2/config"
	"github.com/nats-io/nats.go"
)

type JetstreamStoreFactory struct {
	appServiceKey string
}

func NewJetstreamStoreFactory(appServiceKey string) *JetstreamStoreFactory {
	return &JetstreamStoreFactory{appServiceKey: appServiceKey}
}

type JetstreamStore struct {
	conn       *nats.Conn
	kv         nats.KeyValue
	serviceKey string
}

func (f *JetstreamStoreFactory) NewStore(cfg interfaces.DatabaseInfo, _ config.Credentials) (interfaces.StoreClient, error) {
	conn, err := nats.Connect(fmt.Sprintf("nats://%s:%d", cfg.Host, cfg.Port))

	if err != nil {
		return nil, err
	}

	js, err := conn.JetStream()

	if err != nil {
		return nil, err
	}

	kv, err := js.KeyValue(f.appServiceKey)

	if err != nil {
		kv, err = js.CreateKeyValue(&nats.KeyValueConfig{Bucket: f.appServiceKey})
	}

	return &JetstreamStore{
		conn:       conn,
		serviceKey: f.appServiceKey,
		kv:         kv,
	}, err
}

func (j JetstreamStore) Store(o interfaces.StoredObject) (id string, err error) {
	err = o.ValidateContract(false)

	if err != nil {
		return id, err
	}

	jsn, err := json.Marshal(o)

	if err != nil {
		return o.ID, err
	}

	_, err = j.kv.Create(o.ID, jsn)

	return o.ID, err
}

func (j JetstreamStore) RetrieveFromStore(appServiceKey string) (objects []interfaces.StoredObject, err error) {
	if appServiceKey != j.serviceKey {
		return nil, fmt.Errorf("invalid service key '%s', configured for '%s'", appServiceKey, j.serviceKey)
	}
	keys, _ := j.kv.Keys() // ignore no keys error

	for _, key := range keys {
		var so interfaces.StoredObject
		e, err := j.kv.Get(key)

		err = json.Unmarshal(e.Value(), &so)

		if err != nil {

		}

		objects = append(objects, so)
	}

	return objects, err
}

func (j JetstreamStore) Update(o interfaces.StoredObject) error {
	err := o.ValidateContract(true)

	if err != nil {
		return err
	}

	jsn, err := json.Marshal(o)

	if err != nil {
		return err
	}

	_, err = j.kv.Put(o.ID, jsn)

	return err
}

func (j JetstreamStore) RemoveFromStore(o interfaces.StoredObject) error {
	err := o.ValidateContract(true)

	if err != nil {
		return err
	}

	return j.kv.Delete(o.ID)
}

func (j JetstreamStore) Disconnect() error {
	j.conn.Close()
	j.kv = nil

	return nil
}
