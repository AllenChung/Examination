package map_reduce

import (
	"log"
	"os"
	"bufio"
)

func ReadFileAsLines(fileName string) []string {
	var emptyFileNames []string
	f, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
		return emptyFileNames
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	lines := []string{}
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 {
			lines = append(lines, scanner.Text())
		}
	}
	return lines
}