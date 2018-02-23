package queue

import "testing"

func Test_internalLogger(t *testing.T) {

	debugOutput := false
	errorOutput := false
	debug := func(...interface{}) {
		debugOutput = true
	}
	error := func(...interface{}) {
		errorOutput = true
	}

	SetDebugLogger(debug)
	SetErrorLogger(error)

	logger.Debug("test")
	logger.Error("test")

	if debugOutput != true {
		t.Fatalf("Expected custom debug function to be used")
	}

	if errorOutput != true {
		t.Fatalf("Expected custom error function to be used")
	}

	// reset tests
	debugOutput = false
	errorOutput = false

	SetDebugLogger(nil)
	SetErrorLogger(nil)

	logger.Debug("test")
	logger.Error("test")

	if debugOutput != false {
		t.Fatalf("Expected custom debug function to be reset")
	}

	if errorOutput != false {
		t.Fatalf("Expected custom error function to be reset")
	}
}