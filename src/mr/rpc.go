package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type JobType int8

const (
	MAP_JOB_TYPE    JobType = 1
	REDUCE_JOB_TYPE JobType = 2
)

const stdlog = false

func (jobType JobType) String() string {
	switch jobType {
	case MAP_JOB_TYPE:
		return "MapJob"
	case REDUCE_JOB_TYPE:
		return "ReduceJob"
	default:
		return "unknown"
	}
}

type GetJobArgs struct {
}

type GetJobReply struct {
	Job  *jobDetail
	Done bool
}

type SubmitJobArgs struct {
	JobType JobType
	SrcFile string
	File    string
}
type SubmitJobReply struct {
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
