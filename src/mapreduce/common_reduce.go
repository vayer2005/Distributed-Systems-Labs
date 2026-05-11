package mapreduce

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"sort"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	var kvs []KeyValue

	for mi := 0; mi < nMap; mi++ {
		fn := reduceName(jobName, mi, reduceTaskNumber)
		f, err := os.Open(fn)
		if err != nil {
			continue // map task produced nothing for this reduce partition
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				log.Fatal("doReduce Decode:", err)
			}
			kvs = append(kvs, kv)
		}
		_ = f.Close()
	}

	sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })

	of, err := os.Create(outFile)
	if err != nil {
		log.Fatal("doReduce Create:", err)
	}
	defer of.Close()

	enc := json.NewEncoder(of)

	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		var vals []string
		for k := i; k < j; k++ {
			vals = append(vals, kvs[k].Value)
		}
		if err := enc.Encode(KeyValue{kvs[i].Key, reduceF(kvs[i].Key, vals)}); err != nil {
			log.Fatal("doReduce Encode:", err)
		}
		i = j
	}
}
