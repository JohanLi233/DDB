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

func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
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
			cl := client.MakeClient(net.IP(ip), port)
			clients = append(clients, cl)
		}
	}
	me := len(clients)
	fmt.Println(me)
	cl := client.MakeClient(GetOutboundIP(), os.Args[1])
	clients = append(clients, cl)
	persister := raft.MakePersister()
	kv := kvraft.StartKVServer(clients, me, persister, 0, os.Args[1])
	log.Println("ok")
	for !kv.Killed() {
		time.Sleep(1 * time.Second)
	}
}
