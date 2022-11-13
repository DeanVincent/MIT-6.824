package shardctrler

import "log"

const Debug = true

func DPrintf(format string, a ...interface{}) (n int) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
