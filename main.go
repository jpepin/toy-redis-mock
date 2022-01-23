package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	addr := "localhost:6379"
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Problem opening TCP listener: %+v", err)
	}
	defer ln.Close()
	l := log.Default()
	l.Printf("Listening on %s", addr)
	// set up in-memory data store
	ds := NewDataStore()

	// handle shutdown signals gracefully
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan,
		syscall.SIGHUP,  // kill -SIGHUP XXXX
		syscall.SIGINT,  // kill -SIGINT XXXX or Ctrl+c
		syscall.SIGQUIT, // kill -SIGQUIT XXXX
		syscall.SIGTERM,
		os.Interrupt,
	)

	// Set a low default for connections allowed,
	// since this is probably running on a local machine
	maxConnections := 3
	newConnsAllowed := make(chan bool, maxConnections)
	for i := 0; i < maxConnections; i++ {
		newConnsAllowed <- true
	}
	l.Printf("Max connections set to %d", maxConnections)

	// Shutdown signal channels for each connection
	connHandlers := make(map[chan struct{}]bool)
	// new connections will be added here
	newConns := make(chan *ConnHandler)
	for {
		select {
		case sig := <-signalChan:
			log.Printf("Received signal %v, closing connections", sig)
			for c := range connHandlers {
				c <- struct{}{}
			}
			fmt.Printf("Finished signalling handlers, exiting")
			os.Exit(0)
		case ch := <-newConns:
			go func() {
				defer func() {
					newConnsAllowed <- true
					delete(connHandlers, ch.shutdown)
					l.Printf("Active connections: %d", len(connHandlers))
				}()
				shutdownChan := make(chan struct{})
				connHandlers[shutdownChan] = true
				ch.shutdown = shutdownChan
				l.Printf("Active connections: %d", len(connHandlers))
				ch.Handle(&ds)
			}()
		case <-newConnsAllowed:
			go func() {
				log.Printf("Listening for connections...")
				conn, err := ln.Accept()
				// don't handle connection errors, just bail
				if err != nil {
					l.Fatalf("Problem opening TCP conn: %+v", err)
					return
				}
				nc := &ConnHandler{
					conn: conn,
				}
				newConns <- nc
			}()
		default:
		}

	}
}
