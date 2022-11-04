package main

import (
	"bufio"
	"log"
	"os"
	"unicode"
)

func alldigits(str string) bool {
	for _, ch := range str {
		if !unicode.IsDigit(ch) {
			return false
		}
	}
	return true
}

func main() {
	path := os.Args[1]
	file, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	newlines := []string{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 8 && alldigits(line[:8]) {
			newlines = append(newlines, "1 "+line)
		} else {
			newlines = append(newlines, line)
		}
	}

	file, _ = os.Create(path)
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, line := range newlines {
		w.WriteString(line + "\n")
	}
	w.Flush()
}
