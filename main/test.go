package main

import (
	"DDB/client"
	"DDB/kvraft"
	"fmt"
	"time"

	"io"
	"os"
)

func main() {
	clients := []*client.Client{}
	cl1 := client.MakeClient("192.168.0.7", "1000")
	// clients = append(clients, cl1)
	// cl2 := client.MakeClient("192.168.0.7", "1001")
	clients = append(clients, cl1)
	cl2 := client.MakeClient("192.168.0.109", "1234")
	clients = append(clients, cl2)
	client := kvraft.MakeClerk(clients)
	op := Operator{}
	op.client = client
	file, err := os.Open("pg-being_ernest.txt")
	if err != nil {
	}
	content, err := io.ReadAll(file)
	if err != nil {
	}
	file.Close()
	bt := time.Now()
	op.put(file.Name(), string(content))
	op.put("1", string(content))
	op.put("2", string(content))
	op.put("3", string(content))
	op.put("4", string(content)+string(content)+string(content)+string(content)+string(content))
	op.put(
		"0",
		"0",
	)
	fmt.Println(time.Since(bt))
}

func Chunks(s string, chunkSize int) []string {
	length := len(s)
	if length == 0 {
		return nil
	}
	if chunkSize >= length {
		return []string{s}
	}
	var chunks []string = make([]string, 0, (length-1)/chunkSize+1)
	currentLen := 0
	currentStart := 0
	for i := range s {
		if currentLen == chunkSize {
			chunks = append(chunks, s[currentStart:i])
			currentLen = 0
			currentStart = i
		}
		currentLen++
	}
	chunks = append(chunks, s[currentStart:])
	return chunks
}

func (op *Operator) put(key string, value string) {
	if len(value) > 50000 {
		tb := time.Now()
		op.client.PutAppend(key, "", "Put")
		strings := Chunks(value, 50000)
		fmt.Println(time.Since(tb))
		for _, myString := range strings {
			op.client.PutAppend(key, myString, "Append")
		}
		return
	}
	op.client.PutAppend(key, value, "Put")
}

type Operator struct {
	client *kvraft.Clerk
}

func (op *Operator) append(key string, value string) {
	op.client.PutAppend(key, value, "Append")
}

func (op *Operator) get(key string) string {
	value := op.client.Get(key)
	if value == "" {
		fmt.Println("\"\"")
		return value
	}
	return value
}

func (op *Operator) writeToFile(key string) {
	file, _ := os.Create(key)
	file.WriteString(op.get(key))
	file.Close()
}
