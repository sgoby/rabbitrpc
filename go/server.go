package rabbitrpc

import (
	"github.com/streadway/amqp"
	"encoding/json"
	"context"
	"sync"
	"log"
	"reflect"
	"fmt"
	"time"
)

type RpcMode string

//
const (
	MODE_BALANCED  RpcMode = "balanced"
	MODE_BROADCAST RpcMode = "broadcast"
)

//
type rpcMethod struct {
	rpcServer *RpcServer
	queue     *amqp.Queue
	method    interface{}
	channel   *amqp.Channel
	mContext  context.Context
	cancel    func()
}

type RpcServer struct {
	conn         *amqp.Connection
	rpcMap       map[string]*rpcMethod
	mContext     context.Context
	cancel       func()
	group        *sync.WaitGroup
	exchange     string
	mode         RpcMode
	onRun        bool
	notifyClosed chan *amqp.Error
	url          string
}

//amqp://guest:guest@localhost:5672/
func NewRpcServer(url, exchange string) (*RpcServer, error) {
	var err error
	rs := &RpcServer{
		rpcMap:   make(map[string]*rpcMethod),
		group:    new(sync.WaitGroup),
		mode:     MODE_BALANCED,
		exchange: exchange,
		url:url,
	}
	rs.mContext, rs.cancel = context.WithCancel(context.Background())
	rs.conn, err = rs.connect()
	if err != nil {
		return nil, err
	}
	return rs, err
}
//
func (s *RpcServer) reConnect() {
	for {
		conn, err := s.connect()
		if err != nil {
			fmt.Println(err)
			<-time.After(time.Second * 3)
			continue
		}
		log.Println("ReConnect success .....")
		s.conn = conn
		break
	}
	//update method register
	for method, rm := range s.rpcMap {
		rm.close()
		s.Register(method,rm.method)
	}

}
//
func (s *RpcServer) connect() (*amqp.Connection, error) {
	conn, err := amqp.Dial(s.url)
	if err != nil {
		return nil, err
	}
	s.notifyClosed = make(chan *amqp.Error)
	conn.NotifyClose(s.notifyClosed)
	return conn, err
}

//
func (s *RpcServer) SetMode(mode RpcMode) {
	s.mode = mode
}

//func(argus ...interface{}) error
func (s *RpcServer) Register(method string, call interface{}) (err error) {
	val := reflect.ValueOf(call)
	if val.Type().Kind() != reflect.Func {
		return fmt.Errorf("the call must be func.")
	}
	rm, err := s.newRpcMethod(method)
	if err != nil {
		return err
	}
	rm.method = call
	s.rpcMap[method] = rm
	if s.isRun() {
		s.group.Add(1)
		go rm.run()
	}
	return nil
}

//
func (s *RpcServer) newRpcMethod(method string) (*rpcMethod, error) {
	ch, err := s.conn.Channel()
	if err != nil {
		return nil, err
	}
	err = ch.Qos(10, 0, false)
	if err != nil {
		ch.Close()
		return nil, err
	}
	err = ch.ExchangeDeclare(s.exchange, "direct", true, false, false, true, nil)
	if err != nil {
		ch.Close()
		return nil, err
	}
	//
	var mQueue amqp.Queue
	if s.mode == MODE_BROADCAST {
		mQueue, err = ch.QueueDeclare("", false, true, false, false, nil)
		if err != nil {
			ch.Close()
			return nil, err
		}
	} else {
		mQueue, err = ch.QueueDeclare(method, false, true, false, false, nil)
		if err != nil {
			ch.Close()
			return nil, err
		}
	}
	err = ch.QueueBind(mQueue.Name, method, s.exchange, false, nil)
	if err != nil {
		ch.Close()
		return nil, err
	}
	rm := &rpcMethod{queue: &mQueue, rpcServer: s, channel: ch}
	rm.mContext, rm.cancel = context.WithCancel(s.mContext)
	return rm, nil
}
func (s *RpcServer) isRun() bool {
	return s.onRun
}

//
func (s *RpcServer) Run() {
	s.group.Add(1)
	for _, rm := range s.rpcMap {
		s.group.Add(1)
		go rm.run()
	}
	go s.closeListen()
	s.onRun = true
	s.group.Wait()
}

//
func (s *RpcServer) closeListen() {
	for {
		select {
		case <-s.notifyClosed:
			s.reConnect()
		}
	}
}
//
func (s *RpcServer) Close() {
	if s.cancel != nil {
		s.cancel()
	}
	//
	for _, rm := range s.rpcMap {
		if rm != nil {
			rm.close()
		}
	}
	s.group.Done()
	//
	if s.conn != nil {
		s.conn.Close()
	}
}

//
func (re *rpcMethod) close() {
	if re.cancel != nil {
		re.cancel()
	}
	if re.channel != nil {
		re.channel.Close()
	}
}

//
func (re *rpcMethod) run() {
	if re != nil && re.rpcServer != nil {
		defer re.rpcServer.group.Done()
	}
	log.Println(re.queue.Name)
	//re.queue.Name
	msgs, err := re.channel.Consume(re.queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return
	}
	//defer close(msgs)
	for msg := range msgs {
		select {
		case <-re.mContext.Done():
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
		} else {
			responseData, err = json.Marshal(result)
			if err != nil {
				log.Println(err)
			}
		}
		//
		err = re.channel.Publish(re.rpcServer.exchange, msg.ReplyTo, false, false, amqp.Publishing{
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
func (re *rpcMethod) callMethod(param ...interface{}) (response interface{}, err error) {
	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			err = fmt.Errorf(fmt.Sprintf("%v", recoverErr))
		}
	}()
	//
	valMethod := reflect.ValueOf(re.method)
	if valMethod.Type().Kind() != reflect.Func {
		return nil, fmt.Errorf("the call must be func.")
	}
	//
	numIn := valMethod.Type().NumIn()
	//
	var argsTypes []reflect.Type
	for i := 0; i < numIn; i++ {
		argsTypes = append(argsTypes, valMethod.Type().In(i))
	}
	argus, err := getValues(argsTypes, param...)
	if err != nil {
		return nil, err
	}
	//
	if len(argus) < numIn {
		return nil, fmt.Errorf("the param count not enough %d", numIn)
	}
	var reVals []reflect.Value
	//可变参数
	if valMethod.Type().IsVariadic() {
		reVals = valMethod.Call(argus)
	} else {
		reVals = valMethod.Call(argus[:numIn])
	}
	if len(reVals) > 0 {
		if len(reVals) > 1 {
			if err, ok := reVals[1].Interface().(error); ok {
				return reVals[0].Interface(), err
			}
		}
		return reVals[0].Interface(), nil
	}
	return nil, nil
}

//
func getValues(types []reflect.Type, param ...interface{}) (vals []reflect.Value, err error) {
	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			err = fmt.Errorf(fmt.Sprintf("%v", recoverErr))
		}
	}()
	if len(param) < len(types) {
		return nil, fmt.Errorf("the param count not enough %d", len(types))
	}
	vals = make([]reflect.Value, 0, len(param))
	for i, p := range param {
		val := reflect.ValueOf(p)
		val = val.Convert(types[i])
		vals = append(vals, val)
	}
	return vals, nil
}
