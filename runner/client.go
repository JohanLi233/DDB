package main

import (
	"6.5840/client"
	"6.5840/kvraft"
	"fmt"
)

func main() {
	clients := []*client.Client{}
	cl1 := client.MakeClient("192.168.0.109")
	clients = append(clients, cl1)
	cl2 := client.MakeClient("192.168.0.7")
	clients = append(clients, cl2)
	client := kvraft.MakeClerk(clients)
	client.Put("Jaha", "the best")
	fmt.Println(client.Get("Jaha"))
}
