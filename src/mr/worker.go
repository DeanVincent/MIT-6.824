package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

//hash
// use hash(key) % NReduce to choose to reduce task number for each KeyValue emitted by Map.
func hash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		reply := doPullTask()
		DPrintf("Worker: receive coordinator's task %v \n", reply)

		switch reply.Action {
		case MapAction:
			doMapTask(mapf, reply)
		case ReduceAction:
			doReduceTask(reducef, reply)
		case WaitAction:
			time.Sleep(WaitTime)
		case CompleteAction:
			return
		}
	}
}

func doMapTask(mapf func(string, string) []KeyValue, reply *PullTaskReply) {
	inputFileName := reply.MapInputFile
	nReduce := reply.NReduce

	inputFile, err := os.Open(inputFileName)
	if err != nil {
		log.Fatalf("cannot open file %v", inputFileName)
	}
	inputContent, err := ioutil.ReadAll(inputFile)
	if err != nil {
		log.Fatalf("cannot read file %v", inputFileName)
	}
	inputFile.Close()
	kva := mapf(inputFileName, string(inputContent))
	intermediates := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		i := hash(kv.Key) % nReduce
		intermediates[i] = append(intermediates[i], kv)
	}
	var wg sync.WaitGroup
	for i, intermediate := range intermediates {
		wg.Add(1)
		go func(reduceId int, intermediate []KeyValue) {
			defer wg.Done()
			var buf bytes.Buffer
			enc := json.NewEncoder(&buf)
			for _, kv := range intermediate {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode json %v", kv)
				}
			}
			outputFileName := generateMapOutputFileName(reply.Id, reduceId)
			err := atomicWriteFile(outputFileName, &buf)
			if err != nil {
				log.Fatalf("cannot write file %v", outputFileName)
			}
		}(i, intermediate)
	}
	wg.Wait()
	doReportTask(reply.Action, reply.Id)
}

func doReduceTask(reducef func(string, []string) string, reply *PullTaskReply) {
	nMap := reply.NMap
	reduceId := reply.Id

	var kva []KeyValue
	for mapId := 0; mapId < nMap; mapId++ {
		inputFileName := generateMapOutputFileName(mapId, reduceId)
		inputFile, err := os.Open(inputFileName)
		if err != nil {
			log.Fatalf("cannot open file %v", inputFileName)
		}
		dec := json.NewDecoder(inputFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		inputFile.Close()
	}

	result := make(map[string][]string)
	for _, kv := range kva {
		result[kv.Key] = append(result[kv.Key], kv.Value)
	}

	var buf bytes.Buffer
	for k, vs := range result {
		n := reducef(k, vs)
		fmt.Fprintf(&buf, "%v %v\n", k, n)
	}
	atomicWriteFile(generateResultName(reduceId), &buf)
	doReportTask(reply.Action, reply.Id)
}

func doPullTask() *PullTaskReply {
	args := PullTaskArgs{}
	reply := PullTaskReply{}
	call("Coordinator.PullTask", &args, &reply)
	return &reply
}

func doReportTask(action ActionType, id int) *ReportTaskReply {
	args := ReportTaskArgs{Action: action, Id: id}
	reply := ReportTaskReply{}
	call("Coordinator.ReportTask", &args, &reply)
	return &reply
}

// call
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func atomicWriteFile(fileName string, r io.Reader) (err error) {
	f, err := os.CreateTemp("", fileName)
	if err != nil {
		fmt.Errorf("cannot create file %v", f.Name())
	}
	defer func() {
		if err != nil {
			_ = os.Remove(f.Name())
		}
	}()
	defer f.Close()
	tempName := f.Name()
	if _, err := io.Copy(f, r); err != nil {
		fmt.Errorf("cannot write data to tempFile %v: %v", tempName, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("cannot close tempFile %v: %v", tempName, err)
	}

	info, err := os.Stat(fileName)
	if os.IsNotExist(err) {

	} else if err != nil {
		return err
	} else {
		if err := os.Chmod(tempName, info.Mode()); err != nil {
			return fmt.Errorf("cannot set filemode on tempfile %q: %v", tempName, err)
		}
	}
	if err := os.Rename(tempName, fileName); err != nil {
		return fmt.Errorf("cannot rename %q with tempfile %q: %v", fileName, tempName, err)
	}
	return nil
}
