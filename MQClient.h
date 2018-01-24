
/***************************************************************************************
 *
 * Copyright (c) 2014 Lejent, Inc. All Rights Reserved
 *
 **************************************************************************************/
/**
 *    @file: MQClient.h
 *  @author: zhangxingbiao(xingbiao.zhang@lejent.com)
 *    @date: 01/12/2017 01:50:09 PM
 * @version: 1.0 
 *   @brief: 
 *  
 **/
#ifndef __MQ_CLIENT_H__
#define __MQ_CLIENT_H__
#include <pthread.h>
#include <SimpleAmqpClient/SimpleAmqpClient.h>
#include "Mutex.h"

class Channel;

const int DM_NONPERSISTENT = 1;
const int DM_PERSISTENT = 2;

class Consumer {
  public:
    Consumer(std::string queue, Channel &channel) : 
      _channel(channel), _queue(queue){
    }
  
    /**
    * @brief 拉取消息，timeout为超时
    * @param [out] message 消息
    * @param [in] timeout 超时，单位毫秒
    * @return 成功为0，失败为-1
    **/
    int pull(std::string &message, int timeout);
    
    /**
    * @brief 对拉取消息的确认，根据pull的返回值来确定，
    * 如果pull失败后执行ack会抛异常。
    **/
    void ack();
  
  private:
    Channel &_channel;
    std::string _queue;
    AmqpClient::Envelope::ptr_t _envelope; 
};

class Producer {
  public:
    Producer(std::string queue,  Channel &channel) : 
      _channel(channel), _queue(queue) {
    }
    
    /**
    * @brief 推送消息
    * @param [in] message 消息
    * @return 成功为0，失败为-1
    **/
    int push(const std::string &message, int deliver_mode = DM_NONPERSISTENT);
  
  private:
    Channel &_channel;
    std::string _queue;
};

class Exchange {
  public:
    Exchange(std::string name, Channel &channel) : _channel(channel), _name(name) {
    }

    void bind_queue(std::string queue);

    int push(const std::string &message, int deliver_mode = DM_NONPERSISTENT);

  private:
    Channel &_channel;
    std::string _name;
};

class Channel {
  public:
    Channel(std::string host, std::string port, std::string user_name, std::string password, std::string vhost) {
      _host = host;
      _port = port;
      _user_name = user_name;
      _password = password;
      _vhost = vhost;
      _uri = "amqp://" + user_name + ":" + password + "@" + host + ":" + port + "/" + vhost;
    }

    Channel(std::string uri) {
      _uri = uri;
    }

    /**
    * @brief 打开通道,使用前必须先open
    **/
    int open();

    void close() {
      
    }

    /**
    * @brief 对消息确认
    **/
    void ack(const AmqpClient::Envelope::ptr_t &envelope) {
      _channel->BasicAck(envelope);
    }
  
    
    /**
    * @brief 从队列中拉取一个消息
    **/
    int consume_message(AmqpClient::Envelope::ptr_t &envelope, const std::string &queue, int timeout);
    
    /**
    * @brief 发送一个消息
    **/
    int publish(const std::string &message, const std::string &queue, int deliver_mode, const std::string &exchange_name = "");
    
    /**
    * @brief 把queue绑定到exchange_name上
    **/
    void bind_queue(std::string queue, std::string exchange_name);

    /**
    * @brief 创建一个消费者
    **/
    Consumer create_consumer(std::string queue_name);
    
    /**
    * @brief 创建一个生产者
    **/
    Producer create_producer(std::string queue_name);
    
    /**
    * @brief 创建一个Exchange, fanout类型
    **/
    Exchange create_exchange(std::string exchanger_name);
    
    void cancel_consumer(std::string queue);
    
    void rebuild() { _rebuild();}

  private:
    
    /**
    * @brief 如果队列持续超过20min中为空，则SimpleClient会抛出socket error
    * 异常，这个时候需要重新连接MQ，重新建立所有已经注册的生产者和消费者。
    **/
    void _rebuild();
    Consumer _create_consumer(std::string queue_name);
    Producer _create_producer(std::string queue_name);
    Exchange _create_exchange(std::string name);
    void _cancel_consumer(std::string queue);
    Mutex _mutex; //因为channel可能异步重建所有生产者消费者，所以需要有锁.
    std::string _uri;
    std::string _host;
    std::string _port;
    std::string _user_name;
    std::string _password;
    std::string _vhost;
    AmqpClient::Channel::ptr_t _channel;
    //给人眼看的只是队列名，例如队列名为ocr，但是SimpleClient会生成一个对应的tag,
    //例如:amq.ctag-B0yEXhbbrTFvOjyeQTS09w，所以维护一个映射，简化客户端使用方法。
    std::map<std::string, std::string> _consumer_name; //记录所有的消费者，每个消费者对应唯一的tag
    std::map<std::string, std::string> _producer_name; //记录所有的生产者，每个生产者对应唯一的tag
    std::map<std::string, std::vector<std::string> > _exchange_name;
};

#endif
