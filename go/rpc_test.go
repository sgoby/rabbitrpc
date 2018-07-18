package rabbitrpc

import (
	"testing"
	"fmt"
	"time"
)


var url string = "amqp://username:passwor@localhost:5672/test"
func Test_server(t *testing.T){
	s,err := NewRpcServer(url,"rabbitRPC")
	if err != nil{
		fmt.Println(err)
		return
	}
	err = s.Register("all",rpcMethodTest)
	if err != nil{
		fmt.Println(err)
		return
	}
	s.Run()
}
//
func rpcMethodTest(argus ...interface{}) (interface{},error){
	fmt.Println(argus)
	return "ok",nil
}

//
func Test_Client(t *testing.T){
	c,err := Open(url,"rabbitRPC")
	if err != nil{
		fmt.Println(err)
		return
	}
	//
	begin := time.Now()
	for i:=0; i< 100 ;i++ {
		doCall(c,i)
	}
	end := time.Now()
	fmt.Println(float64(end.UnixNano() - begin.UnixNano()) / float64(time.Second))
}

//
func doCall(c *RpcClient,index int){
	m := "test"
	if index % 2 == 0{
		m = "all"
	}
	re,err := c.Call(m,"klajsdlfkj",fmt.Sprintf("%d",index))
	if err != nil{
		fmt.Println(err)
		return
	}
	fmt.Println(re)
}

