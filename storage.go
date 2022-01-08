package main

import "sync"

type DataStore struct {
	store map[string]string
	lock  sync.RWMutex
}

func NewDataStore() DataStore {
	store := make(map[string]string)
	return DataStore{
		store: store,
		lock:  sync.RWMutex{},
	}
}

func (d *DataStore) Read(key string) (string, bool) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	val, ok := d.store[key]
	return val, ok
}

func (d *DataStore) Write(key, value string) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.store[key] = value
}

func (d *DataStore) Delete(key string) (string, bool) {
	d.lock.Lock()
	defer d.lock.Unlock()
	val, ok := d.store[key]
	if ok {
		delete(d.store, key)
	}
	return val, ok
}
