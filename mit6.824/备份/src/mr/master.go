package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"

type MapTask struct {
	id int
	file string
	startTime time.Time
	done bool

}
type ReduceTask struct {
	id int
	file string
	startTime time.Time
	done bool
}

type Master struct {
	// Your definitions here.
	mutex    sync.Mutex
	maptask []MapTask
	maptaskRemain int
	reducetask []ReduceTask
	reduceRemain int
	

}
func ( master * MakeMaster) GetTask(args *TaskArgs, reply * TaskReply) error{
	master.mutex.Lock()
	defer master.mutex.Unlock()
	switch args.DoneType{
	case TaskTypeMap:
		if(!master[args.Id].done){
			master.MapTask[args.Id].done = true
			for reduced ,file := range master.MapTask[args.ID].file{
				if len(file) > 0 {
					master.ReduceTask[reduced] = append(master.ReduceTask[reduced],file)
				}
			}
			master.maptaskRemain--
		}
	case TaskTypeReduce:
		if(!master[args.Id].Done){
			master[args.Id].Done = true
			master.reduceRemain--
		}
	}

	now :=time.Now()
	timeout := now.Add(-10 * time.Second)
	if master.maptaskRemain > 0{
		for idx := range master.MapTask{
			if !master.MapTask[idx].Done && master.MapTask[idx].startTime.Before(timeout){
				reply.TaskType = TaskTypeMap
				reply.Id = master.MapTask[idx].id
				reply.file = []string(master.MapTask[idx].file)
				reply.NReduce = len(master.ReduceTask)
				master.MakeMaster[idx].startTime = now
				return nil
			}
		}
	}else if master.reduceRemain > 0 {
		for idx := range master.ReduceTask {
			if !master.ReduceTask[idx].Done && master.ReduceTask[idx].startTime.Before(timeout){
				t := master.ReduceTask[idx]
				reply.Id = t.id
				master.MakeMaster[idx].startTime = now
				reply.file = t.file
				reply.TaskType = TaskTypeReduce
				return nil
			}
		}
	}else {
		reply.TaskType = TaskTypeExit
	}
	return nil
	

}
func (master * Master)server(){
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := masterSock()
	os.Remove(sockname)
	l,e := net.Listen("unix",sockname)
	if e != nil{
		log.Fatal("listen error:",e)
	}
	go http.Serve(l,nil)
}

func (master *Master)Done()bool{
	master.mutex.Lock()
	defer master.mutex.Unlock()
	return  master.maptaskRemain == 0&& master.reduceRemain == 0 
}

func MakeMastor(files []string,nReduce int) *Master{
	master:= Master{
		maptask : make([]MapTask,len(file)),
		reducetask : make([]ReduceTask , nReduce),
		maptaskRemain : len(file),
		reduceRemain : nReduce,
	}
	log.Printf(
		"Mastor has %d map tasks and %d redcue tasks to distribute\n",
		master.maptaskRemain,
		master.reduceRemain,
	)
	for i,f := range files{
		master.mapTask[i] = MapTask(id: i, file : f ,done : false)
	}
	for i := 0; i< nReduce ; i++{
		master.reducetask[i] = ReduceTask(id: i , done : false)
	}
	master.server()
	return &master
}
// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.


	m.server()
	return &m
}
