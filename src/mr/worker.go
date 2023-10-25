package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

const WAIT_TIME = 500 * time.Millisecond

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		ok := CallGetTask(&args, &reply)
		if !ok || reply.Type == STOP {
			break
		}

		switch reply.Type {
		case MAP:
			DoMAP(reply.Filenames[0], reply.TaskNum, reply.NReduce, mapf)
			finish_args := FinishTaskArgs{
				Type:    MAP,
				TaskNum: reply.TaskNum,
			}
			finish_reply := FinishTaskReply{}
			CallFinishTask(&finish_args, &finish_reply)
		case REDUCE:
			DoReduce(reply.Filenames, reply.TaskNum, reducef)
			finish_args := FinishTaskArgs{
				Type:    REDUCE,
				TaskNum: reply.TaskNum,
			}
			finish_reply := FinishTaskReply{}
			CallFinishTask(&finish_args, &finish_reply)
		case WAIT:
			time.Sleep(WAIT_TIME)
		default:
			time.Sleep(WAIT_TIME)
		}
	}
}

func DoMAP(filename string, task_num int, reduce_num int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))

	files := make([]*os.File, reduce_num)
	encoders := make([]*json.Encoder, reduce_num)
	for i := 0; i < reduce_num; i++ {
		ofile, err := ioutil.TempFile("", "mr-tmp*")
		if err != nil {
			log.Fatalf("cannot create temp file")
		}
		defer ofile.Close()

		encoder := json.NewEncoder(ofile)
		encoders[i] = encoder
		files[i] = ofile
	}

	var index int
	for _, kv := range kva {
		index = ihash(kv.Key) % reduce_num
		err = encoders[index].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %v", kv)
		}
	}

	for i := 0; i < reduce_num; i++ {
		filename_tmp := fmt.Sprintf("mr-%d-%d", task_num, i)
		err := os.Rename(files[i].Name(), filename_tmp)
		if err != nil {
			log.Fatalf("cannot rename %v to %v", files[i].Name(), filename_tmp)
		}
	}
}

func DoReduce(filenames []string, task_num int, reducef func(string, []string) string) {
	kva := []KeyValue{}
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	ofile, err := ioutil.TempFile("", "mr-out-tmp*")
	if err != nil {
		log.Fatalf("cannot create temp file")
	}
	defer ofile.Close()

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

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	output_filename := fmt.Sprintf("mr-out-%d", task_num)
	err = os.Rename(ofile.Name(), output_filename)
	if err != nil {
		log.Fatalf("cannot rename %v to %v", ofile.Name(), output_filename)
	}
}

func CallGetTask(args *GetTaskArgs, reply *GetTaskReply) bool {
	return call("Coordinator.GetTask", args, reply)
}

func CallFinishTask(args *FinishTaskArgs, reply *FinishTaskReply) bool {
	return call("Coordinator.FinishTask", args, reply)
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
