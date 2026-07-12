package main

import "os"

func verboseLogsEnabled() bool {
	return os.Getenv("LOG_VERBOSE") == "1"
}
