
/***************************************************************************************
 *
 * Copyright (c) 2017 Lejent, Inc. All Rights Reserved
 *
 **************************************************************************************/
/**
 *    @file: MQClient.cpp
 *  @author: zhangxingbiao(xingbiao.zhang@lejent.com)
 *    @date: 01/12/2017 02:01:07 PM
 * @version: 1.0 
 *   @brief: 
 *  
 **/
#include <stdio.h>
#include <exception>
#include "MQClient.h"
#include "AutoLock.h"
#include "Logger.h"

int Channel::open() {
  try {
    _channel = AmqpClient::Channel::CreateFromUri(_uri);
  } catch (std::exception &e) {
    LOG_ERROR(debug_log, "open channel failed! message:%s", e.what());
    return -1;
  }
  return 0;
}

Consumer Channel::create_consumer(std::string queue_name){
  AutoLock<Mutex> lock(&_mutex);
  try {
    Consumer consumer = _create_consumer(queue_name);
    return consumer;
  } catch (std::exception &e) {
    LOG_ERROR(debug_log, "create consumer %s failed, %s", queue_name.c_str(), e.what());
    exit(-1);
  }
}

Consumer Channel::_create_consumer(std::string queue_name){
  std::string consumer_queue = _channel->DeclareQueue(queue_name, true, true, false, false); 
  std::string consumer_name = _channel->BasicConsume(consumer_queue, "", true, false, false); 
  _channel->BasicQos(consumer_name, 1);
  _consumer_name[queue_name] = consumer_name;
  Consumer consumer(queue_name, *this);
  return consumer;
}

Producer Channel::create_producer(std::string queue_name) {
  AutoLock<Mutex> lock(&_mutex);
  try {
    Producer producer = _create_producer(queue_name);
    return producer;
  } catch (std::exception &e) {
    LOG_ERROR(debug_log, "create producer %s failed, %s", queue_name.c_str(), e.what());
    exit(-1);
  }
}

Producer Channel::_create_producer(std::string queue_name) {
  std::string producer_queue = _channel->DeclareQueue(queue_name, false, true, false , false);
  _producer_name[queue_name] = producer_queue;
  Producer producer(queue_name, *this);
  return producer;
}

Exchange Channel::create_exchange(const std::string name) {
  AutoLock<Mutex> lock(&_mutex);
  try {
    Exchange exchange = _create_exchange(name);
    return exchange;
  } catch(std::exception &e) {
    LOG_ERROR(debug_log, "create exchange %s failed, %s", name.c_str(), e.what());
    exit(-1);
  }
}

Exchange Channel::_create_exchange(std::string name) {
  _channel->DeclareExchange(name, AmqpClient::Channel::EXCHANGE_TYPE_FANOUT, false, true, false);
  Exchange exchange(name, *this);
  return exchange;
}

int Channel::consume_message(AmqpClient::Envelope::ptr_t &envelope, const std::string &queue, int time_out) {
  try {
    std::string tag = "";
    std::map<std::string, std::string>::iterator it;
    {
      AutoLock<Mutex> lock(&_mutex);
      it = _consumer_name.find(queue);
      if(it != _consumer_name.end()) {
        tag = it->second;
      }
    }
    bool ret = false;
    if(tag != "") {
      ret = _channel->BasicConsumeMessage(tag, envelope, time_out);
    } else {
      ret = false;
    }
    return ret == true ? 0 : -1;
  } catch (std::exception &e) {
    //20min中会自动断开连接抛出异常，需要重建所有的生产者消费者
    LOG_ERROR(debug_log, "consume message failed, %s", e.what());
    AutoLock<Mutex> lock(&_mutex);
    _rebuild(); 
  }
  return -1;
}

int Channel::publish(const std::string &message, const std::string &queue, int deliver_mode, const std::string &exchange_name) {
  AmqpClient::BasicMessage::ptr_t message_ptr; 
  message_ptr = AmqpClient::BasicMessage::Create(message);
  //必须设置message属性，否则Python客户端无法解析
  message_ptr->ContentType("application/data");
  message_ptr->ContentEncoding("binary");
  //慎用dm_persistent，这会导致publish的时候hang住10秒。此外性能下降的特别厉害。
  switch(deliver_mode) {
    case DM_PERSISTENT:
      message_ptr->DeliveryMode(AmqpClient::BasicMessage::dm_persistent);
      break;
    case DM_NONPERSISTENT:
      message_ptr->DeliveryMode(AmqpClient::BasicMessage::dm_nonpersistent);
      break;
    default:
      break;
  }

  message_ptr->Priority(0);
  try {
    if(exchange_name != "") {
      _channel->BasicPublish(exchange_name, "", message_ptr);
      return 0;
    }

    std::string tag = "";
    std::map<std::string, std::string>::iterator it;
    {
      AutoLock<Mutex> lock(&_mutex);
      it = _producer_name.find(queue);
      if(it != _producer_name.end()) {
        tag = it->second;
      }
    }

    if(tag != "") {
      _channel->BasicPublish(exchange_name, tag, message_ptr); 
      return 0;
    }
  } catch (AmqpClient::MessageReturnedException &e) {
    //20min中会自动断开连接抛出异常，需要重建所有的生产者消费者
    AutoLock<Mutex> lock(&_mutex);
    LOG_ERROR(debug_log, "MQ exception:%s then rebuild", e.what());
    _rebuild();
  }
  return -1;
}

void Channel::bind_queue(const std::string queue, const std::string exchange) {
  _channel->BindQueue(queue, exchange);
}

void Channel::_rebuild() {
  LOG_ERROR(debug_log, "MQ Channel rebuild!");
  std::map<std::string, std::string>::iterator it;
  //重建所有消费者
  for(it = _consumer_name.begin(); it != _consumer_name.end(); ++it) {
    std::string raw_queue = it->first;
    //如果不cancel所有的consumer会导致有些消息消费不了
    _cancel_consumer(raw_queue);
  }
  
  //需要重新建立一个新channel，否则create consumer会出core
  for(;;) {
    try {
      _channel = AmqpClient::Channel::CreateFromUri(_uri);
      break;
    } catch (std::exception &e) {
      LOG_INFO(debug_log, "Channel create failed: %s, then retry", e.what());
    }
  }
  for(it = _consumer_name.begin(); it != _consumer_name.end(); ++it) {
    std::string raw_queue = it->first;
    //如果不cancel所有的consumer会导致有些消息消费不了
    _create_consumer(raw_queue);
  }

  //重建所有生产者
  for(it = _producer_name.begin(); it != _producer_name.end(); ++it) {
    std::string raw_queue = it->first;
    _create_producer(raw_queue);
  }
  //重建所有exchange
  std::map<std::string, std::vector<std::string> >::iterator ex_it;
  for(ex_it = _exchange_name.begin(); ex_it != _exchange_name.end(); ++ex_it) {
    std::vector<std::string> &queue = ex_it->second;
    _create_exchange(ex_it->first);
    for(int i = 0; i < queue.size(); ++i) {
      bind_queue(queue[i], ex_it->first);
    }
  }
}

void Channel::cancel_consumer(std::string queue) {
  AutoLock<Mutex> mutex(&_mutex);
  _cancel_consumer(queue);
  _consumer_name.erase(queue);
}

void Channel::_cancel_consumer(std::string queue) {
  try {
    if(_consumer_name.find(queue) != _consumer_name.end()) {
      _channel->BasicCancel(_consumer_name[queue]);
    }
  } catch (std::exception &e) {
  
  }
}

int Consumer::pull(std::string &message, int time_out) {
  try {
    int ret = _channel.consume_message(_envelope, _queue, time_out); 
    if(ret == -1) {
      return -1;
    }
    message = _envelope->Message()->Body();
    return 0;
  } catch (std::exception &e) {
    LOG_INFO(debug_log, "consumer pull failed! message=%s", e.what());
  }
}

void Consumer::ack() {
  try {
    _channel.ack(_envelope); 
  } catch (std::exception &e) {
    LOG_INFO(debug_log, "ack failed! message=%s", e.what());
  }
}

int Producer::push(const std::string &message, int deliver_mode) {  
  int ret = _channel.publish(message, _queue, deliver_mode);
  return ret;
}

void Exchange::bind_queue(std::string queue) {
  _channel.bind_queue(queue, _name);
}

int Exchange::push(const std::string &message, int deliver_mode) {
  int ret = _channel.publish(message, "", deliver_mode, _name);
  return ret;
} 

