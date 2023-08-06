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
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("-> ")
		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)
		texts := strings.Split(text, " ")
		if texts[0] == "done" {
			fmt.Println("done")
			break
		}
		if texts[0] == "a" {
			if len(texts) < 2 {
				fmt.Println("Need IP")
				continue
			}
			if len(texts) < 3 {
				fmt.Println("Need port")
				continue
			}
			ip := texts[1]
			port := texts[2]
			cl := client.MakeClient(ip, port)
			clients = append(clients, cl)
		}
	}
	client := kvraft.MakeClerk(clients)
	op := Operator{}
	op.client = client
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
