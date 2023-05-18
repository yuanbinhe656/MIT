package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "sort"
import "time"
import "encoding/json"
import  "io/ioutil"
import  "os"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type MyKey KeyValue
func (m MyKey)Len() int {return len(m)}
func (m MyKey)Swap(i , j int ){ m[i],m[j] = m[j], m[i]}
func (m MyKey)Less(i , j int ){ return m[i].key < m[j].key}


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
		var finishedTask = TaskArgs{DoneType:TaskTypeNone}
		var reply = TaskReply
		for{
		reply=GetTask(&finishedTask)
		switch reply.Type :{
		case TaskTypeMap:
			fielname := reply.files[0]
			nReduce =  reply.NReduce
			file, err := os.Open(fielname)
			if err !=nil{
				log.Fatalf("cannot open the file")
				return
			}
			intermidate := mapf(fielname,file)
			reducecontent := make( map( [int]) []KeyValue)
			for intr range :=intermidate{
				idx := hash(intr.key)
				reducecontent[idx] = append(reducecontent[idx],intr)
			}
			reducefile := make([]string, nReduce)
			for i range :=reducefile {
				reducefile[i] = fmt.Sprintf("mr-%d-%d",reply.idx,i)
			}
			for idx,intr :=range reducecontent{
				if file,err:=os.Create(fielname[idx]) , err !=nil{
					log.Fatalf("cannot creat the file")
					return
				}
				defer file.Close()
				enc := json.NewEncoder(file)
				for _,kv :=range intr{
				err := enc.Encode(kv)
				if (err != nil)
				{
					log.Fatal(err)
				}

				}

				
			}
			finishedTask =TaskArgs(DoneType:TaskTypeMap , Id:reply.Id,Files: reducefile)
		case TaskTypeReduce:
			id := reply.Id
			finalword := []KeyValue{}
			startfiles := reply.Files
			for _,filename := range startfiles{
				file,err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open file")
				}
				dec := json.NewDecoder(file)
				for{
					var kv KeyValue
					err := dec.Decode(&kv)
					if err !=nil{
						break
					}
					finalword = append( finalword , kv)
				}


			}
			sort.Sort(Bykey(finalword))

			fname := fmt.Sprintf("out-%d",reply.Id)
			ofile,_ :=os.Create(fname)
			defer ofile.Close()
			i :=0
			for i <len(finalword){
				j := i+1
				value := []string{}
				value = append(value , finalword[i].value)
				for finalword[i].key == finalword[j].key && j < len(finalword){				
					j ++
					value = append(value ,finalword[j])
				}
				output:= reducef(finalword[i].key,value)
				fmt.Fprintf(ofile,"%v %v\n",finalword[i].key,output)
				i = j
			}
			finishedTask = TaskArgs(DoneType:TaskTypeReduce , Id: reply.Id,Files: fname,)
		case TaskTypeSleep:
			time.Sleep(500 * time.Milliscond)
			finishedTask = TaskArgs(DoneType:TaskTypeNone )
		case TaskTypeExit:
			return
		default:
			panic(fmt.Sprintf("Unknown type %v ",reply.Type))
		}
		
	

		
		
	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

}
	}

func GetTask(taskargs * TaskArgs)TaskReply{
	reply :=TaskReply{}
	ok := call("Master.GetTask",taskargs,reply)
	if !ok{
		fmt.Printf("call failed !\n")
		
	}
	return reply
}
//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
