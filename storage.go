package main

import (
	"fmt"
	"sync"
)

const SimpleStringType = "+"
const IntegerType = ":"
const ErrorType = "-"
const BulkStringType = "$"

type DataStore struct {
	store map[string]BasicObject
	rwmu  sync.RWMutex
}

// BasicObject supports storing primitive types
// supported by redis: Simple Strings, and Integers.
// Unknown values are stored as error type.
type BasicObject struct {
	redisType string
	value     string
}

func (b BasicObject) String() string {
	return fmt.Sprintf("%s%s", b.redisType, b.value)
}

func NewBasic(oType, value string) BasicObject {
	var redisType string
	switch oType {
	case SimpleStringType:
		redisType = SimpleStringType
	case IntegerType:
		redisType = IntegerType
	case BulkStringType:
		redisType = SimpleStringType
	default:
		redisType = ErrorType
	}
	return BasicObject{
		redisType: redisType,
		value:     value,
	}
}

func NewDataStore() DataStore {
	store := make(map[string]BasicObject)
	return DataStore{
		store: store,
		rwmu:  sync.RWMutex{},
	}
}

func (d *DataStore) Read(key string) (string, bool) {
	d.rwmu.RLock()
	defer d.rwmu.RUnlock()
	val, ok := d.store[key]
	return val.String(), ok
}

func (d *DataStore) Write(key, value, oType string) {
	d.rwmu.Lock()
	defer d.rwmu.Unlock()

	d.store[key] = NewBasic(oType, value)
}

func (d *DataStore) Delete(key string) int {
	d.rwmu.Lock()
	defer d.rwmu.Unlock()
	_, ok := d.store[key]
	if ok {
		delete(d.store, key)
		return 1
	}
	return 0
}
