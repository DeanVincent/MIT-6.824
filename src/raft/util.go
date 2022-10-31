package raft

import "log"

// Debugging
const Trace = false
const Debug = true

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func TPrintf(format string, a ...interface{}) {
	if Trace {
		log.Printf(format, a...)
	}
	return
}
