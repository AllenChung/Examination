package main

import (
	"../map_reduce"
	"strings"
	"strconv"
	"log"
)

func main() {
	worker := map_reduce.Worker{MapFunction, ReduceFunction}
	go worker.InitWorker()
	map_reduce.InitMaster("test", 3)
}

func ReduceFunction(key string, values []string) []string {
	sum := 0
	for _, v := range values {
		i, err := strconv.Atoi(v)
		if err != nil {
			log.Fatal(err)
		}
		sum += i
	}
	return []string{strconv.Itoa(sum)}
}

func MapFunction(key string, value string) map[string]string {
	resultInter := make(map[string]int)
	for _, word := range strings.Fields(value) {
		if val, ok := resultInter[word]; ok {
			resultInter[word] = val + 1
		} else {
			resultInter[word] = 1
		}
	}
	result := make(map[string]string)
	for k, v := range resultInter {
		result[k] = strconv.Itoa(v)
	} 
	return result
}