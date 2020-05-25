package map_reduce

import (
	"log"
	"net"
	"net/rpc"
	"net/http"
	"strings"
	"os"
	"bufio"
	"fmt"
)

type Worker struct {
	MapTask func(key string, value string) map[string]string
	ReduceTask func(key string, values[] string) []string 
}

func (w *Worker) Map(arg *MapParameter, reply *map[string]string) error {
	lines := ReadFileAsLines(arg.FileName)
	interKV := w.MapTask(arg.FileName, strings.Join(lines, " "))
	dir := arg.FileName
	os.RemoveAll(dir)
	os.Mkdir(dir, 0755)
	for k, v := range interKV {
		interFileName := dir + "/" + k
		if _, err := os.Stat(interFileName); os.IsNotExist(err) {
			file, err := os.Create(interFileName)
			if err != nil {
				log.Fatal(err)
				return err
			}
			defer file.Close()
			w := bufio.NewWriter(file)
			fmt.Fprintln(w, v)
			defer w.Flush()
		} else {
			f, err := os.OpenFile(interFileName, os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
    			return err
			}
			defer f.Close()
			if _, err = f.WriteString(v); err != nil {
				return err
			}
		}
		(*reply)[k] = interFileName
	}
	return nil
}

func (w *Worker) Reduce(arg *ReduceParameter, reply *map[string]string) error {
	var lines []string
	for _, dir := range arg.Directories {
		lines = append(lines, ReadFileAsLines(dir)...) 
	}
	result := w.ReduceTask(arg.Key, lines)
	dir := "./result/"

	fileName := dir + arg.Key
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer file.Close()
	write := bufio.NewWriter(file)
	fmt.Fprintln(write, result[0])
	defer write.Flush()
	(*reply)[arg.Key] = result[0]
	return nil
}

func (w *Worker) InitWorker() {
	rpc.Register(w)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":8080")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Output(1, "worker starting")
	http.Serve(l, nil)
}

type MapParameter struct {
	FileName string
}

type ReduceParameter struct {
	Key string
	Directories []string
}
