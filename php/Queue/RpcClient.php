<?php
/**
 * Created by PhpStorm.
 * User: LJF
 * Date: 2018/6/27
 * Time: 14:05
 */

namespace App\Queue;
use \AMQPQueue;

class RpcClient extends RabbitMQ{

    private static $instance;
    private $clientQueue;
    private $clientQueueName = "rpcqueue";
    //
    public static function getInstance(){
        if(self::$instance == null){
            self::$instance = new RpcClient();
        }
        return self::$instance;
    }
    //
    private function __construct($exchange = null){
        $conf = array(
            'host'=>env('MQ_HOST'),
            'port'=>env('MQ_PORT'),
            'login'=>env('MQ_USER'),
            'password'=>env('MQ_PWD'),
            'vhost'=>env('MQ_VHOST'),
            'read_timeout' => 10,
            'write_timeout' => 10,
            'connect_timeout' => 10,
        );
        $this->connect($conf);
        $this->clientQueueName = $this->clientQueueName.".".uniqid();
        $this->clientQueue = new AMQPQueue($this->channel);
        $this->clientQueue->setName($this->clientQueueName);
        $this->clientQueue->setFlags(AMQP_AUTODELETE);
        $this->clientQueue->declareQueue();
        $this->clientQueue->bind($this->exchange->getName(),$this->clientQueueName);
    }
    //
    public function callRPC($method,...$args){
        if (!$this->conn->isConnected()) {
            return false;
        }
        //
        $msgId = uniqid();
        $attributes = array(
            "message_id" => $msgId,
            "reply_to" => $this->clientQueueName
        );
        //
        if($this->exchange->publish(json_encode($args),$method,AMQP_NOPARAM,$attributes) == false){
            return false;
        }
        $result = null;
        $this->clientQueue->consume(function ($envelope, $queue) use (&$result,$msgId){
            if($msgId == $envelope->getMessageid()){
                $result = $envelope->getBody();
                $queue->ack($envelope->getDeliveryTag());
                return false;
            }
            $queue->nack($envelope->getDeliveryTag());
        });
        return $result;
    }
}