package client

import (
	"net/rpc"
)

type Client struct {
	Ip string
}

func MakeClient(ip string) *Client {
	cl := &Client{}
	cl.Ip = ip
	return cl
}

func (cl *Client) Call(rpcname string, args interface{}, reply interface{}) bool {
	address := cl.Ip + ":1234"
	c, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err == nil
}
