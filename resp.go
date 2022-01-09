package main

import "fmt"

func parseRawValType(raw string) string {
	if len(raw) == 0 {
		return ErrorType
	}
	typeChar := string(raw[0])
	switch typeChar {
	case SimpleStringType:
		return SimpleStringType
	case IntegerType:
		return IntegerType
	case BulkStringType:
		return BulkStringType
	}
	return ErrorType
}

func formatRESPResponse(message string) []byte {
	return []byte(fmt.Sprintf("%s\r\n", message))
}

func formatRESPError(message string) []byte {
	return formatRESPResponse(fmt.Sprintf("%s%s", ErrorType, message))
}

func formatRESPString(message string) []byte {
	return formatRESPResponse(fmt.Sprintf("%s%s", SimpleStringType, message))
}

func formatRESPInt(message int) []byte {
	return formatRESPResponse(fmt.Sprintf("%s%d", IntegerType, message))
}

func checkSupportedRESPType(valType string) error {
	if valType[0] != bulkString {
		fmt.Printf("Received unsupported input '%b'\n", valType[0])
		return fmt.Errorf("%b type not supported", valType[0])
	}
	return nil
}
