package main

import (
	"bufio"
	"fmt"
	"net"
)

func main() {
	ln, err := net.Listen("tcp", "localhost:6379")
	if err != nil {
		fmt.Printf("Problem opening TCP listener: %+v\n", err)
		return
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("Problem opening TCP conn: %+v\n", err)
			return
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	data, err := r.ReadBytes('\n')
	if err != nil {
		fmt.Printf("Problem reading conn: %+v\n", err)
		return
	}
	fmt.Printf("Received %s\n", data)
}
