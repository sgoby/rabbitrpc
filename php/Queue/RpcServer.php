<?php
/**
 * Created by PhpStorm.
 * User: LJF
 * Date: 2018/6/27
 * Time: 14:04
 */

namespace App\Queue;
use \AMQPQueue;

class RpcServer extends RabbitMQ{
    //
    public function __construct($exchange = null){
        $conf = array(
            'host'=>env('MQ_HOST'),
            'port'=>env('MQ_PORT'),
            'login'=>env('MQ_USER'),
            'password'=>env('MQ_PWD'),
            'vhost'=>env('MQ_VHOST'),
            'write_timeout' => 10,
            'connect_timeout' => 10,
        );
        if($this->connect($conf)){
            $this->startServer();
        }
    }
    //
    public function registerRPC($method,callable $func){
        if (!$this->conn->isConnected()) {
            return false;
        }
        //
        $queue = new AMQPQueue($this->channel);
        $queue->setName($method);
        $queue->setFlags(AMQP_AUTODELETE);
        $queue->declareQueue();
        if($queue->bind($this->exchange->getName(),$method) == false){
            return false;
        }
        $queue->consume(function ($envelope, $queue) use ($func){
            $body = $envelope->getBody();
            if(!is_callable($func)){
                return;
            }
            $msg = json_decode($body);
            $data = $msg;
            if(!is_array($data)){
                $data = array($msg);
            }
            $responseData = $func(...$data);
            $attributes = array(
                "message_id" => $envelope->getMessageid()
            );
            $this->exchange->publish($responseData,$envelope->getReplyto(),AMQP_NOPARAM,$attributes);
            $queue->ack($envelope->getDeliveryTag());
        });
    }
}