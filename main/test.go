package main

import (
	"DDB/client"
	"DDB/kvraft"
	"bytes"
	"fmt"
	"io"
	"os"
)

func main() {
	clients := []*client.Client{}
	cl1 := client.MakeClient("192.168.0.109")
	clients = append(clients, cl1)
	cl2 := client.MakeClient("192.168.0.7")
	clients = append(clients, cl2)
	// cl3 := client.MakeClient("192.168.0.177")
	// clients = append(clients, cl3)
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
	op.put(file.Name(), string(content))
	op.put("1", string(content))
	op.put("2", string(content))
	op.put("3", string(content))
	op.put("4", string(content)+string(content)+string(content)+string(content)+string(content))
	op.put(
		"0",
		"0",
	)
}

func SplitSubN(s string, n int) []string {
	sub := ""
	subs := []string{}

	runes := bytes.Runes([]byte(s))
	l := len(runes)
	for i, r := range runes {
		sub = sub + string(r)
		if (i+1)%n == 0 {
			subs = append(subs, sub)
			sub = ""
		} else if (i + 1) == l {
			subs = append(subs, sub)
		}
	}

	return subs
}

func (op *Operator) put(key string, value string) {
	op.client.PutAppend(key, "", "Put")
	if len(value) > 10000 {
		strings := SplitSubN(value, 10000)
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
