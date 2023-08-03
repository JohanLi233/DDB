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

	} else if os.Args[1] == "append" {

	} else {
		fmt.Println("Unknown operation")
		os.Exit(1)
	}

}

func put(client kvraft.Clerk, key string, value string) {

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

func kvAppend(client kvraft.Clerk, key string, value string) {

}

func writeToFile(client kvraft.Clerk, key string, fileName string) {}
