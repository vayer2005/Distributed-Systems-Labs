package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"log"
	"os"
)

// doMap manages one map task: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	data, err := os.ReadFile(inFile)
	if err != nil {
		log.Fatal("doMap ReadFile:", err)
	}
	kvs := mapF(inFile, string(data))

	files := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	defer func() {
		for _, f := range files {
			if f != nil {
				_ = f.Close()
			}
		}
	}()

	for r := 0; r < nReduce; r++ {
		name := reduceName(jobName, mapTaskNumber, r)
		f, err := os.Create(name)
		if err != nil {
			log.Fatal("doMap Create:", err)
		}
		files[r] = f
		encoders[r] = json.NewEncoder(f)
	}

	for _, kv := range kvs {
		r := ihash(kv.Key) % nReduce
		if err := encoders[r].Encode(&kv); err != nil {
			log.Fatal("doMap Encode:", err)
		}
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
