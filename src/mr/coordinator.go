package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int

const (
	IDLE TaskStatus = iota
	PROGRESS_PHASE
	COMPLETED_PHASE
)

type Task struct {
	tno       int
	filenames []string
	status_   TaskStatus
	startTime time.Time
}

type CoordinatorStatus int

const (
	MAP_PHASE CoordinatorStatus = iota
	REDUCE_PHASE
	FINISH_PHASE
)

type Coordinator struct {
	task_       []Task
	reduce_num_ int
	map_num_    int
	status_     CoordinatorStatus
	mu_         sync.Mutex
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu_.Lock()
	defer c.mu_.Unlock()

	finish_flag := c.IsAllFinish()
	if finish_flag {
		c.NextPhase()
	}
	for i := 0; i < len(c.task_); i++ {
		if c.task_[i].status_ == IDLE {
			reply.Err = Success
			reply.TaskNum = i
			reply.Filenames = c.task_[i].filenames
			if c.status_ == MAP_PHASE {
				reply.Type = MAP
				reply.NReduce = c.reduce_num_
			} else if c.status_ == REDUCE_PHASE {
				reply.NReduce = 0
				reply.Type = REDUCE
			} else {
				log.Fatal("unexpected status_")
			}
			c.task_[i].startTime = time.Now()
			c.task_[i].status_ = PROGRESS_PHASE
			return nil
		} else if c.task_[i].status_ == PROGRESS_PHASE {
			curr := time.Now()
			if curr.Sub(c.task_[i].startTime) > time.Second*10 {
				reply.Err = Success
				reply.TaskNum = i
				reply.Filenames = c.task_[i].filenames
				if c.status_ == MAP_PHASE {
					reply.Type = MAP
					reply.NReduce = c.reduce_num_
				} else if c.status_ == REDUCE_PHASE {
					reply.NReduce = 0
					reply.Type = REDUCE
				} else {
					log.Fatal("unexpected status_")
				}
				c.task_[i].startTime = time.Now()
				return nil
			}
		}
	}
	reply.Err = Success
	reply.Type = WAIT
	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mu_.Lock()
	defer c.mu_.Unlock()
	if args.TaskNum >= len(c.task_) || args.TaskNum < 0 {
		reply.Err = ParaErr
		return nil
	}
	c.task_[args.TaskNum].status_ = COMPLETED_PHASE
	if c.IsAllFinish() {
		c.NextPhase()
	}
	return nil
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Init(files []string, reduce_num_ int) {
	c.mu_.Lock()
	defer c.mu_.Unlock()

	task_ := make([]Task, len(files))
	for i, file := range files {
		task_[i].tno = i
		task_[i].filenames = []string{file}
		task_[i].status_ = IDLE
	}

	c.task_ = task_
	c.reduce_num_ = reduce_num_
	c.map_num_ = len(files)
	c.status_ = MAP_PHASE
}

func (c *Coordinator) MakeReduceTasks() {
	task_ := make([]Task, c.reduce_num_)
	for i := 0; i < c.reduce_num_; i++ {
		task_[i].tno = i
		files := make([]string, c.map_num_)
		for j := 0; j < c.map_num_; j++ {
			filename := fmt.Sprintf("mr-%d-%d", j, i)
			files[j] = filename
		}
		task_[i].filenames = files
		task_[i].status_ = IDLE
	}
	c.task_ = task_
}

func (c *Coordinator) IsAllFinish() bool {
	for i := len(c.task_) - 1; i >= 0; i-- {
		if c.task_[i].status_ != COMPLETED_PHASE {
			return false
		}
	}
	return true
}

func (c *Coordinator) NextPhase() {
	if c.status_ == MAP_PHASE {
		c.MakeReduceTasks()
		c.status_ = REDUCE_PHASE
	} else if c.status_ == REDUCE_PHASE {
		c.status_ = FINISH_PHASE
	}
}

func (c *Coordinator) Done() bool {
	c.mu_.Lock()
	defer c.mu_.Unlock()
	if c.status_ == FINISH_PHASE {
		return true
	}
	return false
}

func MakeCoordinator(files []string, reduce_num_ int) *Coordinator {
	c := Coordinator{}

	c.Init(files, reduce_num_)

	c.server()
	return &c
}
