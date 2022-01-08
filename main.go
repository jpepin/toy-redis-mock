package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
)

var set = "set"
var get = "get"
var del = "del"
var bulkString byte = '$'
var nullBulkString = "$-1\r\n"

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
	scanner := bufio.NewScanner(conn)
	// Set the split function for the scanning operation.
	scanner.Split(ScanCRLF)

	reportedError := false
	for scanner.Scan() {
		data := scanner.Text()
		fmt.Printf("fake-redis received: %s\n", data)
		switch {
		// this is a RESP Array
		case data[0] == '*':
			continue
		// RESP Bulk Strings
		case data[0] == '$':
			continue
		// The client requesting available commands
		case data == "COMMAND":
			conn.Write([]byte("*-1\r\n"))
		// NULL Bulk string: "$-1\r\n"
		// NULL array: "*-1\r\n"
		case data == set:
			cErr := HandleSetCall(conn, scanner)
			if cErr != nil {
				fmt.Printf("Problem writing to connection: %+v\n", cErr)
				return
			}
			reportedError = false
		case data == get:
			cErr := HandleGetCall(conn, scanner)
			if cErr != nil {
				fmt.Printf("Problem writing to connection: %+v\n", cErr)
				return
			}
			reportedError = false
		case data == del:
			cErr := HandleDelCall(conn, scanner)
			if cErr != nil {
				fmt.Printf("Problem writing to connection: %+v\n", cErr)
				return
			}
			reportedError = false
		default:
			fmt.Printf("Received unsupported input '%s'\n", data)
			// avoid spamming error until we receive a good input again
			if !reportedError {
				_, conErr := conn.Write(formatSimpleString("not supported"))
				if conErr != nil {
					fmt.Printf("Problem writing to connection: %+v\n", conErr)
					return
				}
				reportedError = true
			}
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("Problem reading input: %s", err)
	}
	fmt.Printf("Done reading from conn\n")
}

func HandleSetCall(conn net.Conn, scanner *bufio.Scanner) error {
	fmt.Printf("Set operation called\n")
	// determine type of key being set
	if scanner.Scan() {
		valType := scanner.Text()
		fmt.Printf("Got key type '%s'\n", valType)
		if vErr := checkSupportedRESPType(valType); vErr != nil {
			conn.Write(formatSimpleString(vErr.Error()))
			return vErr
		}
	}
	var key string
	var value string
	if scanner.Scan() {
		key = scanner.Text()
		fmt.Printf("Got key '%s'\n", key)
	}
	// determine type of value being set
	if scanner.Scan() {
		valType := scanner.Text()
		fmt.Printf("Got value type '%s'\n", valType)
		if vErr := checkSupportedRESPType(valType); vErr != nil {
			conn.Write(formatSimpleString(vErr.Error()))
			return vErr
		}
	}
	if scanner.Scan() {
		value = scanner.Text()
		fmt.Printf("Got value '%s'\n", value)
	}
	sErr := handleSet(key, value)
	if sErr != nil {
		conn.Write(formatSimpleString(sErr.Error()))
	}
	n, cErr := conn.Write(formatSimpleString("OK"))
	if cErr != nil {
		fmt.Printf("Problem writing to connection: %+v\n", cErr)
		return cErr
	}
	fmt.Printf("Wrote %d bytes to conn\n", n)
	return nil
}

func formatSimpleString(message string) []byte {
	return []byte(fmt.Sprintf("+%s\r\n", message))
}

func handleSet(key, value string) error {
	if key == "" || value == "" {
		fmt.Printf("Received unsupported input '%s', '%s'\n", key, value)
		return fmt.Errorf("unsupported command")
	}
	fmt.Printf("Will set %s as %s\n", key, value)
	err := setter(value)
	if err != nil {
		return err
	}
	return nil
}

func HandleGetCall(conn net.Conn, scanner *bufio.Scanner) error {
	fmt.Printf("Get operation called\n")
	// determine type of key being set
	if scanner.Scan() {
		valType := scanner.Text()
		fmt.Printf("Got key type '%s'\n", valType)
		if vErr := checkSupportedRESPType(valType); vErr != nil {
			return vErr
		}
	}
	var key string
	if scanner.Scan() {
		key = scanner.Text()
		fmt.Printf("Got key '%s'\n", key)
	}
	// TODO: handle types
	val, ok, sErr := getter(key)
	if sErr != nil {
		return sErr
	}
	var cErr error
	var n int
	if ok {
		n, cErr = conn.Write(formatSimpleString(val))
	} else {
		// return an explicit nil
		n, cErr = conn.Write([]byte(nullBulkString))
	}
	if cErr != nil {
		fmt.Printf("Problem writing to connection: %+v\n", cErr)
		return cErr
	}
	fmt.Printf("Wrote %d bytes to conn\n", n)
	return nil
}

func getter(key string) (string, bool, error) {
	fmt.Printf("Will get %s\n", key)
	if key == "x" {
		return "fake", true, nil
	}
	return "", false, nil
}

func HandleDelCall(conn net.Conn, scanner *bufio.Scanner) error {
	fmt.Printf("Delete operation called\n")
	// determine type of key being set
	if scanner.Scan() {
		valType := scanner.Text()
		fmt.Printf("Got key type '%s'\n", valType)
		if vErr := checkSupportedRESPType(valType); vErr != nil {
			return vErr
		}
	}
	var key string
	if scanner.Scan() {
		key = scanner.Text()
		fmt.Printf("Got key '%s'\n", key)
	}
	// TODO: handle types
	val, ok, sErr := deleter(key)
	if sErr != nil {
		return sErr
	}
	var cErr error
	var n int
	// TODO: handle types
	if ok {
		n, cErr = conn.Write(formatSimpleString(val))
	} else {
		// return an explicit nil
		n, cErr = conn.Write([]byte(nullBulkString))
	}
	if cErr != nil {
		fmt.Printf("Problem writing to connection: %+v\n", cErr)
		return cErr
	}
	fmt.Printf("Wrote %d bytes to conn\n", n)
	return nil
}

func deleter(key string) (string, bool, error) {
	fmt.Printf("Will delete %s\n", key)
	return "1", true, nil
}

func setter(val string) error {
	// TODO
	fmt.Printf("Will set %s\n", val)
	return nil
}

func checkSupportedRESPType(valType string) error {
	if valType[0] != bulkString {
		fmt.Printf("Received unsupported input '%b'\n", valType[0])
		return fmt.Errorf("%b type not supported", valType[0])
	}
	return nil
}

// Custom scanner code to handle redis carriage-return + newline terminator
// credit: https://stackoverflow.com/a/37531472
// dropCR drops a terminal \r from the data.
func dropCR(data []byte) []byte {
	if len(data) > 0 && data[len(data)-1] == '\r' {
		return data[0 : len(data)-1]
	}
	return data
}

func ScanCRLF(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.Index(data, []byte{'\r', '\n'}); i >= 0 {
		// We have a full newline-terminated line.
		return i + 2, dropCR(data[0:i]), nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), dropCR(data), nil
	}
	// Request more data.
	return 0, nil, nil
}
