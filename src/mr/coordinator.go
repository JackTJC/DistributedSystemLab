package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	nReduce       int
	mapCh         chan *jobDetail //map任务管道，初始和重试的map任务都放入其中
	reduceCh      chan *jobDetail //reduce任务管道
	mapDoneMap    sync.Map        //记录map任务是否完成
	reduceDoneMap sync.Map        //同上
	reduceTasks   [][]KeyValue    //记录分片后的reduce任务
	segmentLock   []sync.Mutex    //reducesTasks的分段锁
}

// Your code here -- RPC handlers for the worker to call.

// 获取任务
func (c *Coordinator) GetJob(args *GetJobArgs, reply *GetJobReply) error {
	defer func() {
		log.Printf("get job reply: type=%v, file=%v, done=%v", reply.Job.Type, reply.Job.File, reply.Done)
	}()
	reply.Job = &jobDetail{}
	if c.Done() { // 所有任务已经完成，直接返回
		reply.Done = true
		return nil
	}
	select {
	case jd := <-c.mapCh:
		reply.Job = jd
		go c.jobChecker(jd, 10*time.Second)
		return nil
	case jd := <-c.reduceCh:
		reply.Job = jd
		go c.jobChecker(jd, 10*time.Second)
		return nil
	default:
		log.Println("no job currently.")
	}
	return nil
}

// 提交任务
func (c *Coordinator) SubmitJob(args *SubmitJobArgs, reply *SubmitJobReply) error {
	log.Printf("submit job type: %v, src file: %v, target file: %v", args.JobType, args.SrcFile, args.File)
	switch args.JobType {
	case MAP_JOB_TYPE:
		// 检查是否已经完成,完成直接返回
		v1, _ := c.mapDoneMap.Load(args.SrcFile)
		if v1.(bool) {
			return nil
		}
		f, e := os.Open(args.File)
		if e != nil {
			log.Printf("map job submit failed: %v", e)
			return nil
		}
		//分流到各个reduce任务
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			idx := ihash(kv.Key) % c.nReduce
			c.segmentLock[idx].Lock()
			c.reduceTasks[idx] = append(c.reduceTasks[idx], kv)
			c.segmentLock[idx].Unlock()
		}
		// 标记已完成
		c.mapDoneMap.Store(args.SrcFile, true)
	case REDUCE_JOB_TYPE:
		v2, _ := c.reduceDoneMap.Load(args.SrcFile)
		if v2.(bool) {
			return nil
		}
		// 标记已完成
		c.reduceDoneMap.Store(args.SrcFile, true)
	}
	return nil
}

// crash处理逻辑
// 对于一个任务，在下发后每隔d时间检查其是否完成，没有完成则重新下发
func (c *Coordinator) jobChecker(jd *jobDetail, d time.Duration) {
	for {
		time.Sleep(d)
		var done bool
		switch jd.Type {
		case MAP_JOB_TYPE:
			v, _ := c.mapDoneMap.Load(jd.File)
			done = v.(bool)
		case REDUCE_JOB_TYPE:
			v, _ := c.reduceDoneMap.Load(jd.File)
			done = v.(bool)
		default:
			return
		}
		if !done {
			switch jd.Type {
			case MAP_JOB_TYPE:
				c.mapCh <- jd
				log.Printf("job crashed, file: %v, type: %v, idx: %v", jd.File, MAP_JOB_TYPE, jd.Number)
			case REDUCE_JOB_TYPE:
				c.reduceCh <- jd
				log.Printf("job crashed, file: %v, type: %v, idx: %v", jd.File, REDUCE_JOB_TYPE, jd.Number)
			default:
				log.Println("unknown job type")
			}
			continue
		}
		break
	}
}

// map完成后生成reduce任务
// 每隔d时间，检查map是否完成，完成后，生产reduce任务并下发
func (c *Coordinator) mapStageChecker(d time.Duration) {
	for {
		time.Sleep(d)
		if c.mapDone() {
			break
		}
	}
	log.Println("map stage finished, starting reduce...")
	reduceFiles := make([]string, 0)
	for idx, task := range c.reduceTasks {
		fn := fmt.Sprintf("/tmp/reduce_task_%d", idx)
		f, err := os.Create(fn)
		if err != nil {
			log.Fatalf("fatal error: %v", err)
		}
		enc := json.NewEncoder(f)
		for _, kv := range task {
			enc.Encode(kv)
		}
		f.Close()
		reduceFiles = append(reduceFiles, fn)
		c.reduceDoneMap.Store(fn, false)
	}
	go func() {
		for idx, f := range reduceFiles {
			c.reduceCh <- &jobDetail{f, idx, REDUCE_JOB_TYPE}
		}
	}()
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	return c.mapDone() && c.reduceDone()
}

// 检测map阶段是否完成
func (c *Coordinator) mapDone() (mapDone bool) {
	mapDone = true
	c.mapDoneMap.Range(func(file, finish interface{}) bool {
		if !finish.(bool) {
			mapDone = false
			return false
		}
		return true
	})
	return
}

// 检测reduce阶段是否完成
func (c *Coordinator) reduceDone() (reduceDone bool) {
	reduceDone = true
	cnt := 0
	if !c.mapDone() {
		return false
	}
	c.reduceDoneMap.Range(func(file, finish interface{}) bool {
		if !finish.(bool) {
			reduceDone = false
			return false
		}
		cnt++
		return true
	})
	return reduceDone && cnt != 0
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	if !stdlog { // 输出日志到文件
		f, err := os.Create(fmt.Sprintf("coordinator-%d.log", os.Getpid()))
		if err != nil {
			log.Fatal(err)
		}
		log.SetOutput(f)

	}
	c.nReduce = nReduce
	c.mapCh = make(chan *jobDetail)
	c.reduceCh = make(chan *jobDetail)
	c.segmentLock = make([]sync.Mutex, nReduce)
	c.reduceTasks = make([][]KeyValue, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = make([]KeyValue, 0)
	}
	c.mapDoneMap = sync.Map{}
	c.reduceDoneMap = sync.Map{}
	c.server()
	for _, f := range files {
		c.mapDoneMap.Store(f, false)
	}
	go func() {
		for idx, v := range files {
			c.mapCh <- &jobDetail{v, idx, MAP_JOB_TYPE}
		}
	}()
	go c.mapStageChecker(time.Second)
	return &c
}
