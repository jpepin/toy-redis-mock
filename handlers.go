package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"net"
)

var set = "set"
var get = "get"
var del = "del"
var bulkString byte = '$'
var nullBulkString = "$-1\r\n"

type ConnHandler struct {
	conn     net.Conn
	err      error
	shutdown chan struct{}
}

func (ch ConnHandler) Handle(ds *DataStore) {
	defer ch.conn.Close()
	l := log.Default()
	scanner := bufio.NewScanner(ch.conn)
	// Set the split function for the scanning operation.
	scanner.Split(ScanCRLF)

	h := &ScanHandler{
		conn:    ch.conn,
		scanner: scanner,
		ds:      ds,
	}
	for {
		select {
		case <-ch.shutdown:
			log.Printf("Shutdown signal received by conn handler")
			return
		default:
			// TODO: this blocks waiting for data
			if scanner.Scan() {
				sErr := h.HandleScannerInput()
				if sErr != nil {
					l.Printf("Fatal: Problem handling input: %s", sErr.Error())
					return
				}
			}
		}
	}
}

type ScanHandler struct {
	conn                 net.Conn
	scanner              *bufio.Scanner
	reportedNotSupported bool
	ds                   *DataStore
}

func (sh *ScanHandler) HandleScannerInput() error {
	data := sh.scanner.Text()
	switch {
	// this is a RESP Array
	case data[0] == '*':
		return nil
	// RESP Bulk Strings
	case data[0] == '$':
		return nil
	// The client requesting available commands
	case data == "COMMAND":
		sh.conn.Write([]byte(nullBulkString))
	case data == set:
		cErr := sh.HandleSetCall()
		if cErr != nil {
			return fmt.Errorf("problem with set: %+v", cErr)
		}
		sh.reportedNotSupported = false
	case data == get:
		cErr := sh.HandleGetCall()
		if cErr != nil {
			return fmt.Errorf("problem with get: %+v", cErr)
		}
		sh.reportedNotSupported = false
	case data == del:
		cErr := sh.HandleDelCall()
		if cErr != nil {
			return fmt.Errorf("problem with delete: %+v", cErr)
		}
		sh.reportedNotSupported = false
	default:
		log.Default().Printf("Received unsupported input '%s'", data)
		// avoid spamming error until we receive a good input again
		if !sh.reportedNotSupported {
			_, conErr := sh.conn.Write(formatRESPError("not supported"))
			if conErr != nil {
				return fmt.Errorf("problem writing to connection: %+v", conErr)
			}
			sh.reportedNotSupported = true
		}
	}
	return sh.scanner.Err()
}

func (sh *ScanHandler) HandleSetCall() error {
	// determine type of key being set
	if sh.scanner.Scan() {
		valType := sh.scanner.Text()
		if vErr := checkSupportedRESPType(valType); vErr != nil {
			sh.conn.Write(formatRESPError(vErr.Error()))
			return vErr
		}
	}
	var key string
	var value string
	if sh.scanner.Scan() {
		key = sh.scanner.Text()
	}
	// determine type of value being set
	var valType string
	if sh.scanner.Scan() {
		rawType := sh.scanner.Text()
		if vErr := checkSupportedRESPType(rawType); vErr != nil {
			sh.conn.Write(formatRESPError(vErr.Error()))
			return vErr
		}
		valType = parseRawValType(rawType)
	}
	if sh.scanner.Scan() {
		value = sh.scanner.Text()
	}
	sErr := handleSet(key, value, valType, sh.ds)
	if sErr != nil {
		sh.conn.Write(formatRESPError(sErr.Error()))
	}
	_, cErr := sh.conn.Write(formatRESPString("OK"))
	if cErr != nil {
		return fmt.Errorf("problem writing to connection: %+v", cErr)
	}
	return nil
}

func handleSet(key, value, oType string, ds *DataStore) error {
	if key == "" || value == "" {
		log.Default().Printf("Received unsupported input '%s', '%s'", key, value)
		return fmt.Errorf("unsupported input type")
	}
	log.Default().Printf("SET %s as %s", key, value)
	ds.Write(key, value, oType)
	return nil
}

func (sh *ScanHandler) HandleGetCall() error {
	// determine type of key being set
	if sh.scanner.Scan() {
		valType := sh.scanner.Text()
		if vErr := checkSupportedRESPType(valType); vErr != nil {
			return vErr
		}
	}
	var key string
	if sh.scanner.Scan() {
		key = sh.scanner.Text()
	}
	log.Default().Printf("GET %s", key)
	val, ok := sh.ds.Read(key)
	var cErr error
	if ok {
		_, cErr = sh.conn.Write(formatRESPResponse(val))
	} else {
		// return an explicit nil
		_, cErr = sh.conn.Write([]byte(nullBulkString))
	}
	if cErr != nil {
		return fmt.Errorf("problem writing to connection: %+v", cErr)
	}
	return nil
}

func (sh *ScanHandler) HandleDelCall() error {
	// determine type of key being set
	if sh.scanner.Scan() {
		valType := sh.scanner.Text()
		if vErr := checkSupportedRESPType(valType); vErr != nil {
			return vErr
		}
	}
	var key string
	if sh.scanner.Scan() {
		key = sh.scanner.Text()
	}
	log.Default().Printf("DEL %s", key)
	deletedCount := sh.ds.Delete(key)
	var cErr error
	if deletedCount > 0 {
		_, cErr = sh.conn.Write(formatRESPInt(deletedCount))
	} else {
		// return an explicit nil
		_, cErr = sh.conn.Write([]byte(nullBulkString))
	}
	if cErr != nil {
		return fmt.Errorf("problem writing to connection: %+v", cErr)
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
