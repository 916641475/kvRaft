package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

type Pair struct {
	beginTime time.Time
	finish    bool
}

type Coordinator struct {
	nReduce        int
	ptr            *[]string
	mapTaskInfo    map[string]Pair
	reduceTaskInfo map[string]Pair
	mapFinish      bool
	reduceFinish   bool
}

func (c *Coordinator) init(nReduce int, ptr *[]string) {
	c.nReduce = nReduce
	c.ptr = ptr
	c.mapFinish = false
	c.reduceFinish = false
	var initpair Pair = Pair{time.Date(2000, time.January, 1, 1, 0, 0, 0, time.Local), false}
	for i := 0; i < len(*ptr); i++ {
		c.mapTaskInfo[(*ptr)[i]] = initpair
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTaskInfo[strconv.Itoa(i)] = initpair
	}
}

func (c *Coordinator) findAvaliable(tasktype string) string {
	var totalfinish bool = true
	var taskInfo *map[string]Pair
	if tasktype == "map" {
		taskInfo = &c.mapTaskInfo
	} else {
		taskInfo = &c.reduceTaskInfo
	}

	for key, value := range *taskInfo {
		if !value.finish {
			totalfinish = false
			if time.Now().Sub(value.beginTime) >= 10 {
				c.mapTaskInfo[key] = Pair{time.Now(), false}
				return key
			}
		}
	}

	if tasktype == "map" {
		c.mapFinish = totalfinish
	} else {
		c.reduceFinish = totalfinish
	}
	if totalfinish == true {
		return "finish"
	} else {
		return "wait"
	}
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) AssignMap(args byte, reply *string) error {
	*reply = c.findAvaliable("map")
	return nil
}

func (c *Coordinator) AssignReduce(args byte, reply *int) error {
	*reply, _ = strconv.Atoi(c.findAvaliable("reduce"))
	return nil
}

func (c *Coordinator) TaskComplete(args string, reply *byte) error {
	if !c.mapFinish {
		c.mapTaskInfo[args] = Pair{time.Now(), true}
	} else {
		c.reduceTaskInfo[args] = Pair{time.Now(), true}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.reduceFinish
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.init(nReduce, &files)

	c.server()
	return &c
}
