package shardkv

import "log"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
