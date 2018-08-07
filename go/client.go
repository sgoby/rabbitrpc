package rabbitrpc

import (
	"github.com/streadway/amqp"
	"encoding/json"
	"fmt"
	"time"
	"errors"
	"sync"
	"sync/atomic"
)

type RpcClient struct {
	conn        *amqp.Connection
	exchange    string
	timeout     time.Duration
	channelPool []*RpcChannel //chan *RpcChannel//  *list.List
	mu          *sync.Mutex
	maxOpen     int
	maxIdle     int
	maxIdleTime int64
	countOpen   int
	serialNo    uint64
}

//
type RpcChannel struct {
	channel   *amqp.Channel
	queue     *amqp.Queue
	delivery  <-chan amqp.Delivery
	isTemp    bool
	idleBegin time.Time
}

//
var ErrorCallTimeout error = errors.New("call timeout.")
//
func Open(url, exchange string) (*RpcClient, error) {
	var err error
	rc := &RpcClient{
		timeout:     time.Second * 10,
		channelPool: make([]*RpcChannel,0,1000),//list.New(),
		mu:          new(sync.Mutex),
		exchange:    exchange,
		maxOpen:     1000,
		maxIdle:     100,
		maxIdleTime: 120,
	}
	//
	rc.conn, err = amqp.Dial(url)
	if err != nil{
		return nil,err
	}
	for i:= 0;i<=100;i++{
		ch ,err := rc.newChannel()
		if err != nil{
			return rc, err
		}
		rc.channelPool = append(rc.channelPool,ch)
	}
	go rc.releaseIdleChannel()
	return rc, err
}

//
func (c *RpcClient) SetTimeout(t time.Duration) {
	c.timeout = t
}

func (c *RpcClient) SetMaxOpenConns(n int) {
	c.maxOpen = n
}

func (c *RpcClient) releaseIdleChannel() {
	for{
		select {
		case <-time.After(c.timeout):
			c.releaseIdleChannelDC()
		}
	}
}
//
func (c *RpcClient) releaseIdleChannelDC() {
	c.mu.Lock()
	defer c.mu.Unlock()
	//
	if len(c.channelPool) > 0 {
		for i:= 0 ;i< len(c.channelPool);i++{
			ch := c.channelPool[0]
			if ch != nil {
				if time.Now().Unix()-ch.idleBegin.Unix() < c.maxIdleTime {
					break
				}
				ch.channel.Close()
				numFree := len(c.channelPool)
				copy(c.channelPool[0:], c.channelPool[1:])
				c.channelPool = c.channelPool[:numFree-1]
			}
		}
	}
}
//
func (c *RpcClient) releaseChannel(ch *RpcChannel) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ch != nil {
		if ch.isTemp || len(c.channelPool) > c.maxOpen {
			ch.channel.Close()
			return
		}
		ch.idleBegin = time.Now()
		c.channelPool = append(c.channelPool,ch)
	}
}
//
func (c *RpcClient) getChannel() (ch *RpcChannel,err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.channelPool) > 0 {
		lens := len(c.channelPool)
		for i:= 0 ;i< lens;i++{
			//
			ch = c.channelPool[0]
			numFree := len(c.channelPool)
			copy(c.channelPool, c.channelPool[1:])
			c.channelPool = c.channelPool[:numFree-1]
			//
			if ch == nil{
				continue
			}
			if time.Now().Unix()-ch.idleBegin.Unix() > c.maxIdleTime {
				ch.channel.Close()
				continue
			}
			return ch,nil
		}
	}
	ch, err = c.newChannel()
	if err != nil {
		c.countOpen += 1
	}
	if c.countOpen > c.maxOpen {
		ch.isTemp = true
	}
	return ch, err
}

//
func (c *RpcClient) newChannel() (*RpcChannel,error) {
	ch, err := c.conn.Channel()
	if err != nil {
		return nil,err
	}
	err = ch.ExchangeDeclare(c.exchange, "direct", true, false, false, true, nil)
	if err != nil {
		return nil,err
	}
	mQueue, err := ch.QueueDeclare("", false, true, false, false, nil)
	if err != nil {
		return nil,err
	}
	//
	err = ch.QueueBind(mQueue.Name, mQueue.Name, c.exchange, false, nil)
	if err != nil {
		return nil,err
	}
	//
	mDelivery, err := ch.Consume(mQueue.Name, "", true, false, false, false, nil)
	if err != nil {
		return  nil,err
	}
	return &RpcChannel{channel: ch, queue: &mQueue,delivery:mDelivery,idleBegin:time.Now()},nil
}

//
func (c *RpcClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _,ch := range  c.channelPool{
		if ch != nil{
			ch.channel.Close()
		}
	}
	if c.conn != nil {
		c.conn.Close()
	}
}

//
func (c *RpcClient) Call(method string,reV interface{}, argus ...interface{}) (error) {
	requestData, err := json.Marshal(argus)
	if err != nil {
		return  err
	}
	id := fmt.Sprintf("NO%d",atomic.AddUint64(&c.serialNo,1))
	ch, err := c.getChannel()
	if err != nil {
		return  err
	}
	defer c.releaseChannel(ch)
	//
	err = ch.channel.Publish(c.exchange, method, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        requestData,
		ReplyTo:     ch.queue.Name,
		Expiration:  "10000",
		MessageId:   id,
	})
	if err != nil {
		return  err
	}
	//
	for {
		select {
		case msg := <-ch.delivery:
			if msg.MessageId != id {
				msg.Nack(false, true)
				continue
			}
			//msg.Ack(false)
			//var data interface{}
			if len(msg.Body) < 1 {
				return  err
			}
			err = json.Unmarshal(msg.Body, &reV)
			if err != nil {
				return  err
			}
			return  nil
		case <-time.After(c.timeout):
			return  ErrorCallTimeout
		}
	}
	return  nil
}