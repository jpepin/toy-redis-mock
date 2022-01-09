package main

import (
	"log"
	"net"
)

func main() {
	addr := "localhost:6379"
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Problem opening TCP listener: %+v", err)
	}
	l := log.Default()
	l.Printf("Listening on %s", addr)
	// set up in-memory data store
	ds := NewDataStore()
	// very basic concurrency control
	// Low default assumes this is running on a local machine
	maxConns := 6
	l.Printf("Max connections set to %d", maxConns)
	var conns int
	for {
		if conns < maxConns {
			conn, err := ln.Accept()
			if err != nil {
				l.Fatalf("Problem opening TCP conn: %+v", err)
			}
			conns++
			go func() {
				defer func() {
					conns--
					l.Printf("Active connections: %d", conns)
				}()
				handleConnection(conn, &ds)
			}()

			l.Printf("Active connections: %d", conns)
		}

	}
}
