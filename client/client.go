package client

import (
	"log"
	"net"
	"net/rpc"
)

type Client struct {
	Ip   net.IP
	Port string
}

func MakeClient(ip net.IP, port string) *Client {
	cl := &Client{}
	cl.Ip = ip
	cl.Port = port
	return cl
}

func (cl *Client) Call(rpcname string, args interface{}, reply interface{}) bool {
	address := string(cl.Ip) + ":" + cl.Port
	c, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Println(err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err == nil
}
