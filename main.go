package main

import (
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
	ds := NewDataStore()
	// Set a low default for connections allowed,
	// since this is probably running on a local machine
	maxConnections := 3
	l.Printf("Max connections set to %d", maxConnections)
	pool := NewConnPoll(maxConnections, ln)
	pool.RequestNewConnection()

	// handle shutdown signals gracefully
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan,
		syscall.SIGHUP,  // kill -SIGHUP XXXX
		syscall.SIGINT,  // kill -SIGINT XXXX or Ctrl+c
		syscall.SIGQUIT, // kill -SIGQUIT XXXX
		syscall.SIGTERM,
		os.Interrupt,
	)
	l.Printf("Listening on %s", addr)
	for {
		select {
		case sig := <-signalChan:
			log.Printf("Received signal %v, closing connections", sig)
			pool.Shutdown()
			l.Printf("Finished signalling handlers, exiting")
			os.Exit(0)
		case ch := <-pool.connsToHandle:
			go func() {
				defer func() {
					pool.Deregister(ch)
					pool.RequestNewConnection()
					l.Printf("Active connections: %d", pool.ActiveConnections())
				}()
				shutdownChan := make(chan struct{})
				ch.shutdown = shutdownChan
				pool.Register(ch)
				l.Printf("Active connections: %d", pool.ActiveConnections())
				ch.Handle(&ds)
			}()
		case <-pool.newConn:
			go pool.Accept()
		case err := <-pool.errs:
			l.Printf("Error: %v", err)
		default:
		}

	}
}
