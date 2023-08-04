package main

import (
	"DDB/client"
	"DDB/kvraft"
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	clients := []*client.Client{}
	cl1 := client.MakeClient("192.168.0.109")
	clients = append(clients, cl1)
	cl2 := client.MakeClient("192.168.0.7")
	clients = append(clients, cl2)
	client := kvraft.MakeClerk(clients)
	op := Operator{}
	op.client = client
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("-> ")
		text, _ := reader.ReadString('\n')
		// convert CRLF to LF
		text = strings.Replace(text, "\n", "", -1)
		texts := strings.Split(text, " ")
		if texts[0] == "get" {
			if len(texts) < 2 {
				fmt.Println("need value")
				continue
			}
			fmt.Println(op.get(texts[1]))
		} else if texts[0] == "put" {
			if len(texts) < 3 {
				fmt.Println("need value")
				continue
			}
			op.put(texts[1], texts[2])
		} else if texts[0] == "append" {
			if len(texts) < 3 {
				fmt.Println("need value")
				continue
			}
			op.append(texts[1], texts[2])
		} else if texts[0] == "write" {
			if len(texts) < 2 {
				fmt.Println("need value")
				continue
			}
			op.writeToFile(texts[1])
		} else {
			fmt.Println("unknown operation")
		}
	}
}

type Operator struct {
	client *kvraft.Clerk
}

func (op *Operator) append(key string, value string) {
	op.client.PutAppend(key, value, "Append")
}

func (op *Operator) put(key string, value string) {
	op.client.PutAppend(key, value, "Put")
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
