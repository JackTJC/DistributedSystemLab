package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path"
	"sort"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	//log file
	if !stdlog { // 输出日志到文件
		f, err := os.Create(fmt.Sprintf("worker-%d.log", os.Getpid()))
		if err != nil {
			log.Fatal(err)
		}
		log.SetOutput(f)
	}
	log.Printf("running worker, pid:%d ...\n", os.Getpid())
	// 不断获取任务，直到coordinator的Done返回true
	for {
		// 不断请求任务，直到Done为true
		jd, done := CallGetJob()
		if done {
			log.Println("coordinator done, worker: ", os.Getpid(), "exited")
			return
		}
		// 根据任务类型，执行不同逻辑
		switch jd.Type {
		case MAP_JOB_TYPE:
			log.Printf("worker: %d, run map job: file=%s", os.Getpid(), jd.File)
			doMapJob(jd.File, mapf)
		case REDUCE_JOB_TYPE:
			log.Printf("worker: %d, run reduce job: file=%s", os.Getpid(), jd.File)
			doReduceJob(jd.File, jd.Number, reducef)
		default:
			log.Println("unknown job type")
		}
		time.Sleep(time.Second) // 根据实验的guideline，这里应该间隔一秒
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doMapJob(f string, mapf func(string, string) []KeyValue) {
	//打开文本文件
	file, err := os.Open(f)
	if err != nil {
		log.Fatalf("cannot open %v", f)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", f)
	}
	file.Close()
	//执行map任务
	kva := mapf(f, string(content))
	resultFn := fmt.Sprintf("/tmp/map_result_of_%s", path.Base(f))
	file, err = os.Create(resultFn)
	if err != nil {
		log.Fatalf("cannot create file: %v", err)
	}
	//编码到json
	enc := json.NewEncoder(file)
	for _, kv := range kva {
		enc.Encode(&kv)
	}
	file.Close()
	//提交
	CallSubmitJob(f, resultFn, MAP_JOB_TYPE)
}

func doReduceJob(f string, idx int, reducef func(string, []string) string) {
	// 打开json文件并解码
	file, err := os.Open(f)
	if err != nil {
		log.Fatalf("cannot open %v", f)
	}
	dec := json.NewDecoder(file)
	kva := make([]KeyValue, 0)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	file.Close()
	// 排序
	sort.Slice(kva, func(i, j int) bool {
		return strings.Compare(kva[i].Key, kva[j].Key) == -1
	})
	outFn := fmt.Sprintf("mr-out-%d", idx+1)
	file, err = os.Create(outFn)
	if err != nil {
		log.Fatalln("create file failed: ", err)
	}
	// 输出到目标文件
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(file, "%v %v\n", kva[i].Key, output)
		i = j
	}
	file.Close()
	// 提交
	CallSubmitJob(f, outFn, REDUCE_JOB_TYPE)
}

func CallGetJob() (*jobDetail, bool) {
	args := GetJobArgs{}
	reply := GetJobReply{}
	call("Coordinator.GetJob", &args, &reply)
	return reply.Job, reply.Done
}

func CallSubmitJob(src, target string, jobType JobType) {
	args := SubmitJobArgs{}
	reply := SubmitJobReply{}
	args.SrcFile = src
	args.File = target
	args.JobType = jobType
	call("Coordinator.SubmitJob", &args, &reply)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
