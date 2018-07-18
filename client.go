package rabbitrpc

import (
	"github.com/streadway/amqp"
	"encoding/json"
	"fmt"
	"time"
	"errors"
)

type RpcClient struct {
	conn     *amqp.Connection
	exchange string
	channel  *amqp.Channel
	queue    *amqp.Queue
	timeout  time.Duration
	delivery <-chan amqp.Delivery
}
//
var ErrorCallTimeout error = errors.New("call timeout.")

//
func Open(url ,exchange string) (*RpcClient, error) {
	var err error
	rc := &RpcClient{
		timeout:time.Second * 10,
	}
	rc.conn, err = amqp.Dial(url)
	rc.channel, err = rc.conn.Channel()
	if err != nil {
		return nil, err
	}
	err = rc.exchangeDeclare(exchange,"")
	return rc, err
}

//
func (c *RpcClient) SetTimeout(t time.Duration){
	c.timeout = t
}

//
func (c *RpcClient) exchangeDeclare(name, kind string) error {
	if len(kind) <= 0 {
		//direct | topic
		kind = "direct"
	}
	var err error
	c.exchange = name
	err = c.channel.ExchangeDeclare(name, kind, true, false, false, true, nil)
	if err != nil {
		return err
	}
	mQueue, err := c.channel.QueueDeclare("", false, true, false, false, nil)
	if err != nil {
		return err
	}
	c.queue = &mQueue
	//
	return c.channel.QueueBind(c.queue.Name, c.queue.Name, c.exchange, false, nil)
}
//
func (c *RpcClient) Close() {
	if c.channel != nil {
		c.channel.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}
//
func (c *RpcClient) Call(method string, argus ...interface{}) (interface{}, error) {
	requestData, err := json.Marshal(argus)
	if err != nil {
		return nil, err
	}
	id := fmt.Sprintf("%d", time.Now().UnixNano())
	err = c.channel.Publish(c.exchange, method, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        requestData,
		ReplyTo:     c.queue.Name,
		Expiration:  "10000",
		MessageId:   id,
	})
	if err != nil {
		return nil, err
	}
	//
	if c.delivery == nil {
		c.delivery, err = c.channel.Consume(c.queue.Name, "", false, false, false, false, nil)
		if err != nil {
			return nil, err
		}
	}
	//
	for{
		select{
			case msg := <-c.delivery:
				if msg.MessageId != id {
					msg.Nack(false, true)
					continue
				}
				msg.Ack(false)
				var data interface{}
				if len(msg.Body) < 1{
					return nil, err
				}
				err = json.Unmarshal(msg.Body, &data)
				if err != nil {
					return nil, err
				}
				return data, nil
			case <- time.After(c.timeout):
				return nil,ErrorCallTimeout
		}
	}
	return nil, nil
}
