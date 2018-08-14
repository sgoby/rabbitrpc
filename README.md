# RabbitMQ RPC

这是一个对RabbitMQ RPC 进行封装的高性能 golang 的开发包。简单易用，并且支持RabbitMQ断网自动重连。

### Server Example

        import "github.com/sgoby/rabbitrpc/go"

        ...

        var url string = "amqp://username:passwor@localhost:5672/test"
        //rabbitRPC 交换机名称
        s,err := rabbitrpc.NewRpcServer(url,"rabbitRPC")
        if err != nil{
            fmt.Println(err)
            return
        }
        err = s.Register("Hello",rpcMethodHello)
        if err != nil{
            fmt.Println(err)
            return
        }

        ...

        func rpcMethodHello(say string) (string,error){
        	return "rpc response:"+ say,nil
        }

### Client Example

        import "github.com/sgoby/rabbitrpc/go"

        ...

        var url string = "amqp://username:passwor@localhost:5672/test"
        //rabbitRPC 交换机名称
        c,err := rabbitrpc.Open(url,"rabbitRPC")
        if err != nil{
            fmt.Println(err)
            return
        }
        defer c.Close()
        var re string
        err := c.Call("Hello",&re,"I'am rpc Client")
        if err != nil{
            fmt.Println(err)
            return
        }
        fmt.Println("Rpc server response content: "+re)