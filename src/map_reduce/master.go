package map_reduce

import (
	"log"
	"os"
	"fmt"
	"bufio"
	"net/rpc"
)

type Master struct {

}

// split the file into M files (only support utf-8 encoded file)
func SplitFile(fileName string, M int) []string {
	lines := ReadFileAsLines(fileName)

	dir := "./" + fileName + "MapTask/"
	os.RemoveAll(dir)
	os.Mkdir(dir, 0755)

	var resultFileNames []string
	linesCount := len(lines)
	if (linesCount < M) {
		M = linesCount
	}
	length := linesCount / M
	if linesCount % M != 0 {
		length++
	}
	var emptyFileNames []string
	start := 0
	end := length
	for i := 0; i < M; i++ {
		subFileName := fmt.Sprintf(dir + "%s%d", fileName, i)
		file, err := os.Create(subFileName)
		if err != nil {
			log.Fatal(err)
			return emptyFileNames
		}
		defer file.Close()
		w := bufio.NewWriter(file)
		if end > linesCount {
			end = linesCount
		}
		for _, line := range lines[start :end] {
			fmt.Fprintln(w, line)
		}
		defer w.Flush()
		resultFileNames = append(resultFileNames, subFileName)
		start = end
		end += length
	}
	return resultFileNames
}

func InitMaster(fileName string, M int) {
	fileNames := SplitFile(fileName, M)
	keyFileMap := make(map[string][]string)
	for _, f := range fileNames {
		kMap := handleMap(f)
		for k, v := range kMap {
			if val, ok := keyFileMap[k]; ok {
				keyFileMap[k] = append(val, v)
			} else {
				keyFileMap[k] = []string{v}
			}
		}
	}
	log.Println(keyFileMap)

	dir := "./result/"
	os.RemoveAll(dir)
	os.Mkdir(dir, 0755)
	for key, value := range keyFileMap {
		reply := handleReduce(key, value)
		log.Println("final result {}", reply)
	}
}

func handleReduce(key string, directories []string) map[string]string {
	client, err := rpc.DialHTTP("tcp", "localhost:8080")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	var reply map[string]string
	args := &ReduceParameter{key, directories}
	err = client.Call("Worker.Reduce", args, &reply)
	if err != nil {
		log.Fatal("arith error:", err)
	}
	return reply
}

func handleMap(fileName string) map[string]string {
	client, err := rpc.DialHTTP("tcp", "localhost:8080")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	var reply map[string]string
	args := &MapParameter{fileName}
	err = client.Call("Worker.Map", args, &reply)
	if err != nil {
		log.Fatal("arith error:", err)
	}
	return reply
}

