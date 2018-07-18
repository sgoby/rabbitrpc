package rabbitrpc

import (
	"github.com/streadway/amqp"
	"encoding/json"
	"context"
	"sync"
	"log"
	"reflect"
	"fmt"
)
//
type rpcEntity struct {
	rpcServer  *RpcServer
	queue      *amqp.Queue
	method     interface{}
}

type RpcServer struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	rpcMap   map[string]*rpcEntity
	mContext context.Context
	cancel   func()
	group    *sync.WaitGroup
	exchange string
}

//amqp://guest:guest@localhost:5672/
func NewRpcServer(url ,exchange string) (*RpcServer, error) {
	var err error
	rs := &RpcServer{
		rpcMap: make(map[string]*rpcEntity),
		group:  new(sync.WaitGroup),
	}
	rs.mContext, rs.cancel = context.WithCancel(context.Background())
	rs.conn, err = amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	rs.channel, err = rs.conn.Channel()
	if err != nil {
		return nil, err
	}
	err = rs.channel.Qos(10,0, false)
	if err != nil {
		return nil, err
	}
	err = rs.exchangeDeclare(exchange,"")
	return rs, err
}

//func(argus ...interface{}) error
func (s *RpcServer) Register(method string, call interface{}) error {
	val := reflect.ValueOf(call)
	if val.Type().Kind() != reflect.Func{
		return fmt.Errorf("the call must be func.")
	}
	mQueue, err := s.channel.QueueDeclare("", false, true, false, false, nil)
	if err != nil {
		return err
	}
	entity := &rpcEntity{queue: &mQueue, method: call, rpcServer: s}
	s.rpcMap[method] = entity
	err = s.channel.QueueBind(mQueue.Name, method, s.exchange, false, nil)
	if err != nil {
		return err
	}
	return nil
}

//mqbool,fanout[direct | topic]
func (s *RpcServer) exchangeDeclare(name, kind string) error {
	if len(kind) <= 0 {
		kind = "direct"
	}
	s.exchange = name
	return s.channel.ExchangeDeclare(name, kind, true, false, false, true, nil)
}

//
func (s *RpcServer) Run() {
	for _, entity := range s.rpcMap {
		s.group.Add(1)
		go entity.run()
	}
	s.group.Wait()
}

//
func (s *RpcServer) Close() {
	if s.cancel != nil {
		s.cancel()
	}
	if s.channel != nil {
		s.channel.Close()
	}
	if s.conn != nil {
		s.conn.Close()
	}
}

//
func (re *rpcEntity) run() {
	if re != nil && re.rpcServer != nil {
		defer re.rpcServer.group.Done()
	}
	log.Println(re.queue.Name)
	//re.queue.Name
	msgs, err := re.rpcServer.channel.Consume(re.queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return
	}
	//defer close(msgs)
	for msg := range msgs {
		select {
		case <-re.rpcServer.mContext.Done():
			return
		default:
		}
		msg.Ack(false)
		//
		var argus []interface{}
		err := json.Unmarshal(msg.Body, &argus)
		if err != nil {
			log.Println(err)
			continue
		}
		//
		var responseData []byte
		result, err := re.callMethod(argus...)
		if err != nil {
			log.Println(err)
		}else {
			responseData, err = json.Marshal(result)
			if err != nil {
				log.Println(err)
			}
		}
		//
		err = re.rpcServer.channel.Publish(re.rpcServer.exchange, msg.ReplyTo, false, false, amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: msg.CorrelationId,
			Body:          responseData,
			MessageId:     msg.MessageId,
		})
		if err != nil {
			//msg.Nack(false, true)
			log.Println(err)
			continue
		}
	}
}
//
func (re *rpcEntity) callMethod(param ...interface{}) (response interface{},err error){
	defer func(){
		if recoverErr := recover(); recoverErr != nil {
			err = fmt.Errorf(fmt.Sprintf("%v",recoverErr))
		}
	}()
	//
	val := reflect.ValueOf(re.method)
	if val.Type().Kind() != reflect.Func{
		return nil,fmt.Errorf("the call must be func.")
	}
	//
	numIn := val.Type().NumIn()
	argus := getValues(param...)
	if len(argus) < numIn{
		return nil,fmt.Errorf("the param count not enough %d",numIn)
	}
	var reVals []reflect.Value
	if val.Type().IsVariadic(){
		reVals = val.Call(argus)
	}else{
		reVals = val.Call(argus[:numIn])
	}
	if len(reVals) > 0{
		if len(reVals) > 1{
			if err,ok := reVals[1].Interface().(error);ok{
				return reVals[0].Interface(),err
			}
		}
		return reVals[0].Interface(),nil
	}
	return nil,nil
}
func getValues(param ...interface{}) []reflect.Value {
	vals := make([]reflect.Value,0,len(param))
	for i := range param {
		vals = append(vals,reflect.ValueOf(param[i]))
	}
	return vals
}