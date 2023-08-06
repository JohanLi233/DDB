package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"DDB/client"
	"DDB/kvraft"
	"DDB/raft"
)

func GetLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Need port")
		return
	}
	clients := []*client.Client{}
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Add existing servers")
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
	me := len(clients)
	cl := client.MakeClient(GetLocalIP(), os.Args[1])
	clients = append(clients, cl)
	persister := raft.MakePersister()
	kv := kvraft.StartKVServer(clients, me, persister, 0, os.Args[1])
	log.Println("ok")
	for !kv.Killed() {
		time.Sleep(1 * time.Second)
	}
}
