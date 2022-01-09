package main

import (
	"fmt"
	"net"
)

func main() {
	ln, err := net.Listen("tcp", "localhost:6379")
	if err != nil {
		fmt.Printf("Problem opening TCP listener: %+v\n", err)
		return
	}
	// set up in-memory data store
	ds := NewDataStore()
	// very basic concurrency control
	// Low default assumes this is running on a local machine
	maxConns := 6
	var conns int
	for {
		if conns < maxConns {
			conn, err := ln.Accept()
			if err != nil {
				fmt.Printf("Problem opening TCP conn: %+v\n", err)
				return
			}
			conns++
			go func() {
				defer func() { conns-- }()
				handleConnection(conn, &ds)
			}()

			fmt.Printf("Active connections: %d\n", conns)
		}

	}
}
