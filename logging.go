package queue

import "log"

type Log func(...interface{})

type internalLogger struct {
	logDebug Log
	logError Log
}

func (l internalLogger) Debug(v ...interface{}) {
	if l.logDebug != nil {
		l.logDebug(v)
	}
}

func (l internalLogger) Error(v ...interface{}) {
	if l.logError != nil {
		l.logError(v)
	}
}

var logger internalLogger = internalLogger{log.Print, log.Print}

// Sets the package's debug logger. Pass nil to disable debug logging.
func SetDebugLogger(log Log) {
	logger.logDebug = log
}

// Sets the package's error logger. Pass nil to disable error logging.
func SetErrorLogger(log Log) {
	logger.logError = log
}