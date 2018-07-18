<?php
/**
 * Created by PhpStorm.
 * User: LJF
 * Date: 2018/6/26
 * Time: 10:42
 */

namespace App\Queue;
use \AMQPConnection;
use \AMQPExchange;
use \AMQPChannel;

class RabbitMQ{
    //
    protected $conn = null;
    protected $exchange;
    protected $channel;
    //
    protected $exchangeName = "rabbitRPC";
    //
    protected function connect(array $conf){
        if(isset($conf['exchange'])){
            $this->exchangeName = $conf['exchange'];
            unset($conf['exchange']);
        }
        //
        $this->conn = new AMQPConnection($conf);
        if (!$this->conn->connect()) {
            return false;
        }
        $this->channel = new AMQPChannel($this->conn);
        $this->exchange = new AMQPExchange($this->channel);
        //交换机名
        $this->exchange->setName($this->exchangeName);
        $this->exchange->setType(AMQP_EX_TYPE_DIRECT);
        //持久化
        //$this->exchange->setFlags(AMQP_DURABLE);
        return true;
    }
    //
    protected function close(){
        if (!$this->conn->isConnected()) {
            return false;
        }
        $this->conn->disconnect();
    }
    //
    protected function startServer(){
        if (!$this->conn->isConnected()) {
            return false;
        }
        $this->exchange->declareExchange();
    }
}