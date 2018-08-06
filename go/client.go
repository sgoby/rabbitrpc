package rabbitrpc

import (
	"github.com/streadway/amqp"
	"encoding/json"
	"fmt"
	"time"
	"errors"
	"sync"
	"container/list"
)

type RpcClient struct {
	conn        *amqp.Connection
	exchange    string
	timeout     time.Duration
	channelPool *list.List
	mu          *sync.Mutex
	maxOpen     int
	maxIdle     int
	maxIdleTime int64
	countOpen   int
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
		channelPool: list.New(),
		mu:          new(sync.Mutex),
		exchange:    exchange,
		maxOpen:     500,
		maxIdle:     100,
		maxIdleTime: 10,
	}
	rc.conn, err = amqp.Dial(url)
	return rc, err
}

//
func (c *RpcClient) SetTimeout(t time.Duration) {
	c.timeout = t
}

func (c *RpcClient) SetMaxOpenConns(n int) {
	c.maxOpen = n
}

//
func (c *RpcClient) releaseChannel(ch *RpcChannel) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ch != nil {
		defer func() { c.countOpen -= 1 }()
		if ch.isTemp || c.channelPool.Len() > c.maxOpen {
			ch.channel.Close()
			return
		}
		ch.idleBegin = time.Now()
		c.channelPool.PushBack(ch)
	}
}

//
func (c *RpcClient) getChannel() (ch *RpcChannel,err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var element *list.Element
	for {
		if c.channelPool.Len() < 1 {
			break;
		}
		element = c.channelPool.Front()
		c.channelPool.Remove(element)
		if element != nil {
			ch = element.Value.(*RpcChannel)
			if time.Now().Unix()-ch.idleBegin.Unix() > c.maxIdleTime {
				ch.channel.Close()
				element = nil
				continue
			}
			break
		}
	}
	//
	if element != nil && element.Value != nil {
		c.countOpen += 1
		return ch, nil
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
	return &RpcChannel{channel: ch, queue: &mQueue}, nil
}

//
func (c *RpcClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		element := c.channelPool.Front()
		if element == nil {
			break
		}
		ch := element.Value.(*RpcChannel)
		ch.channel.Close()
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
	id := fmt.Sprintf("%d", time.Now().UnixNano())
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
	if ch.delivery == nil {
		ch.delivery, err = ch.channel.Consume(ch.queue.Name, "", false, false, false, false, nil)
		if err != nil {
			return  err
		}
	}
	//
	for {
		select {
		case msg := <-ch.delivery:
			if msg.MessageId != id {
				msg.Nack(false, true)
				continue
			}
			msg.Ack(false)
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
