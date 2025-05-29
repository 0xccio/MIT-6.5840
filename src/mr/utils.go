package mr

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
)

func MidFileAssign(reduceNum int) []string {
	var res []string
	path, _ := os.Getwd()
	rd, _ := os.ReadDir(path)
	for _, fi := range rd {
		if strings.HasPrefix(fi.Name(), "mr-mid-") && strings.HasSuffix(fi.Name(), "-"+strconv.Itoa(reduceNum)) {
			res = append(res, fi.Name())
		}
	}
	return res
}

func readFromLocalFile(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return kva
}