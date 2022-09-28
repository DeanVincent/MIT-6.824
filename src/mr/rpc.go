package mr

import "os"
import "strconv"

type PullTaskArgs struct {
}

type PullTaskReply struct {
	Action       ActionType
	Id           int
	NMap         int
	NReduce      int
	MapInputFile string
}

type ReportTaskArgs struct {
	Action ActionType
	Id     int
}

type ReportTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
