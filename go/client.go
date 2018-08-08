package rabbitrpc

import (
	"github.com/streadway/amqp"
	"encoding/json"
	"fmt"
	"time"
	"errors"
	"sync"
	"sync/atomic"
	"context"
	"log"
)

type RpcClient struct {
	conn         *amqp.Connection
	url          string
	exchange     string
	timeout      time.Duration
	channelPool  []*RpcChannel //chan *RpcChannel//  *list.List
	mu           *sync.Mutex
	maxOpen      int
	maxIdle      int
	maxIdleTime  int64
	countOpen    int
	serialNo     uint64
	notifyClosed chan *amqp.Error
	mContext     context.Context
	cancel       func()
}

//
type RpcChannel struct {
	channel      *amqp.Channel
	queue        *amqp.Queue
	delivery     <-chan amqp.Delivery
	isTemp       bool
	idleBegin    time.Time
	mContext     context.Context
	cancel       func()
	notifyClosed chan *amqp.Error
	closed       bool
}

//
var ErrorCallTimeout error = errors.New("call timeout.")

//
func Open(url, exchange string) (*RpcClient, error) {
	var err error
	rc := &RpcClient{
		timeout:      time.Second * 10,
		channelPool:  make([]*RpcChannel, 0, 1000), //list.New(),
		mu:           new(sync.Mutex),
		exchange:     exchange,
		maxOpen:      1000,
		maxIdle:      100,
		maxIdleTime:  120,
		url:          url,
		notifyClosed: make(chan *amqp.Error),
	}
	//
	rc.mContext, rc.cancel = context.WithCancel(context.Background())
	rc.conn, err = rc.connect()
	if err != nil {
		return nil, err
	}
	for i := 0; i <= 100; i++ {
		ch, err := rc.newChannel()
		if err != nil {
			return rc, err
		}
		rc.channelPool = append(rc.channelPool, ch)
	}
	go rc.releaseIdleChannel()
	return rc, err
}

//
func (c *RpcClient) SetTimeout(t time.Duration) {
	c.timeout = t
}

//
func (c *RpcClient) SetMaxOpenConns(n int) {
	c.maxOpen = n
}

//
func (c *RpcClient) releaseIdleChannel() {
	for {
		select {
		case <-c.mContext.Done():
			return
		case <-c.notifyClosed:
			c.reConnect()
		case <-time.After(c.timeout):
			c.releaseIdleChannelDC()
		}
	}
}

//
func (c *RpcClient) reConnect() {
	c.mu.Lock()
	c.clearChannel()
	c.mu.Unlock()
	//
	for {
		conn, err := c.connect()
		if err != nil {
			fmt.Println(err)
			<-time.After(time.Second * 3)
			continue
		}
		log.Println("ReConnect success .....")
		c.conn = conn
		break
	}
}

//
func (c *RpcClient) connect() (*amqp.Connection, error) {
	conn, err := amqp.Dial(c.url)
	if err != nil {
		return nil, err
	}
	c.notifyClosed = make(chan *amqp.Error)
	conn.NotifyClose(c.notifyClosed)
	return conn, err
}

//
func (c *RpcClient) releaseIdleChannelDC() {
	c.mu.Lock()
	defer c.mu.Unlock()
	//
	if len(c.channelPool) > 0 {
		for i := 0; i < len(c.channelPool); i++ {
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
	if ch != nil  {
		if ch.isTemp || len(c.channelPool) > c.maxOpen {
			ch.channel.Close()
			return
		}
		if !ch.closed {
			ch.idleBegin = time.Now()
			c.channelPool = append(c.channelPool, ch)
		}
	}
}

//
func (c *RpcClient) getChannel() (ch *RpcChannel, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.channelPool) > 0 {
		lens := len(c.channelPool)
		for i := 0; i < lens; i++ {
			//
			ch = c.channelPool[0]
			numFree := len(c.channelPool)
			copy(c.channelPool, c.channelPool[1:])
			c.channelPool = c.channelPool[:numFree-1]
			//
			if ch == nil {
				continue
			}
			if time.Now().Unix()-ch.idleBegin.Unix() > c.maxIdleTime {
				ch.channel.Close()
				continue
			}
			return ch, nil
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
func (c *RpcClient) newChannel() (*RpcChannel, error) {
	ch, err := c.conn.Channel()
	if err != nil {
		return nil, err
	}
	err = ch.ExchangeDeclare(c.exchange, "direct", true, false, false, true, nil)
	if err != nil {
		return nil, err
	}
	mQueue, err := ch.QueueDeclare("", false, true, false, false, nil)
	if err != nil {
		return nil, err
	}
	//
	err = ch.QueueBind(mQueue.Name, mQueue.Name, c.exchange, false, nil)
	if err != nil {
		return nil, err
	}
	//
	mDelivery, err := ch.Consume(mQueue.Name, "", true, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	nCh := &RpcChannel{channel: ch, queue: &mQueue, delivery: mDelivery, idleBegin: time.Now()}
	nCh.mContext, nCh.cancel = context.WithCancel(c.mContext)
	nCh.notifyClosed = make(chan *amqp.Error, 1)
	nCh.channel.NotifyClose(nCh.notifyClosed)
	return nCh, nil
}

//
func (c *RpcClient) clearChannel() {
	lens := len(c.channelPool)
	for i := 0; i < lens; i++ {
		//
		ch := c.channelPool[0]
		numFree := len(c.channelPool)
		copy(c.channelPool, c.channelPool[1:])
		c.channelPool = c.channelPool[:numFree-1]
		if ch != nil {
			ch.channel.Close()
		}
	}
}

//
func (c *RpcClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clearChannel()
	if c.conn != nil {
		c.conn.Close()
	}
	if c.cancel != nil {
		c.cancel()
	}
}

//
func (ch *RpcChannel) close(exchange string) {
	if ch.channel != nil && ch.queue != nil {
		ch.channel.QueueUnbind(ch.queue.Name, ch.queue.Name, exchange, nil)
		ch.channel.QueueDelete(ch.queue.Name, true, true, true)
	}
	if ch.channel != nil {
		ch.channel.Close()
	}
}

//
func (c *RpcClient) Call(method string, reV interface{}, argus ...interface{}) (error) {
	requestData, err := json.Marshal(argus)
	if err != nil {
		return err
	}
	id := fmt.Sprintf("NO%d", atomic.AddUint64(&c.serialNo, 1))
	ch, err := c.getChannel()
	if err != nil {
		return err
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
		return err
	}
	//
	for {
		select {
		case <- ch.mContext.Done():
			return fmt.Errorf("Channel is closed")
		case err,_ := <-ch.notifyClosed:
			ch.closed = true
			return fmt.Errorf("Channel is closed: %v", err)
		case msg := <-ch.delivery:
			if msg.MessageId != id {
				//msg.Ack(false)
				continue
			}
			//msg.Ack(false)
			if len(msg.Body) < 1 {
				return err
			}
			err = json.Unmarshal(msg.Body, &reV)
			if err != nil {
				return err
			}
			return nil
		case <-time.After(c.timeout):
			return ErrorCallTimeout
		}
	}
	return nil
}
