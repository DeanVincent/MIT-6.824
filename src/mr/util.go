package mr

import (
	"fmt"
	"log"
	"time"
)

// SchedulePhase represents the phase of this job
type SchedulePhase uint8

const (
	MapPhase SchedulePhase = iota
	ReducePhase
	CompletePhase
)

// StateType represents the state of task
type StateType uint8

const (
	IdleState StateType = iota
	InProgressState
	CompletedState
)

// ActionType represents the action for worker to do
type ActionType uint8

const (
	MapAction ActionType = iota
	ReduceAction
	WaitAction
	CompleteAction
)

const TaskTime = 10 * time.Second

const WaitTime = 1 * time.Second

func generateMapOutputFileName(mapId, reduceId int) string {
	return fmt.Sprintf("mr-inter-%v-%v", mapId, reduceId)
}

func generateResultName(reduceId int) string {
	return fmt.Sprintf("mr-out-%v", reduceId)
}

const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func max(x, y int) int {
	if x > y {
		return x
	} else {
		return y
	}
}
