package main

import (
	"fmt"
	"net"
)

// TODO: handle concurrent connections with cap
func main() {
	ln, err := net.Listen("tcp", "localhost:6379")
	if err != nil {
		fmt.Printf("Problem opening TCP listener: %+v\n", err)
		return
	}
	// set up in-memory data store
	ds := NewDataStore()
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("Problem opening TCP conn: %+v\n", err)
			return
		}
		go handleConnection(conn, &ds)
	}
}
