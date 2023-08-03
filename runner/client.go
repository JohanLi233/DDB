package main

import (
	"6.5840/client"
	"6.5840/kvraft"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: operation + value + key\n")
		os.Exit(1)
	}
	clients := []*client.Client{}
	cl1 := client.MakeClient("192.168.0.109")
	clients = append(clients, cl1)
	cl2 := client.MakeClient("192.168.0.7")
	clients = append(clients, cl2)
	client := kvraft.MakeClerk(clients)
	if os.Args[1] == "get" {
		get(*client, os.Args[2])
	} else if os.Args[1] == "put" {
		if len(os.Args) < 4 {
			fmt.Println("Need value")
			os.Exit(1)
		}
		put(*client, os.Args[2], os.Args[3])
	} else if os.Args[1] == "append" {
		if len(os.Args) < 4 {
			fmt.Println("Need value")
			os.Exit(1)
		}
		kvAppend(*client, os.Args[2], os.Args[3])
	} else {
		fmt.Println("Unknown operation")
		os.Exit(1)
	}

}

func kvAppend(client kvraft.Clerk, key string, value string) {
	client.PutAppend(key, value, "Append")
}

func put(client kvraft.Clerk, key string, value string) {
	client.PutAppend(key, value, "Put")
}

func get(client kvraft.Clerk, key string) string {
	value := client.Get(key)
	if value == "" {
		fmt.Println("\"\"")
		return value
	}
	fmt.Println(value)
	return value
}

func writeToFile(client kvraft.Clerk, key string, fileName string) {}
