
/***************************************************************************************
 *
 * Copyright (c) 2018 Lejent, Inc. All Rights Reserved
 *
 **************************************************************************************/
/**
 *    @file: SelectAlgorithm.cpp
 *  @author: zhangxingbiao(xingbiao.zhang@lejent.com)
 *    @date: 01/25/2018 10:31:13 AM
 * @version: 1.0 
 *   @brief: 
 *  
 **/
#include "SelectAlgorithm.h"
#include "AutoLock.h"
#include "Logger.h"
#include <cstring>


void WeightSelectAlgorithm::add_addr(const std::string &host, int port) {
  AutoLock<Mutex> lock(&_mutex);
  char addr[32] = {'\0'};
  snprintf(addr, sizeof(addr) - 1, "%s:%d", host.c_str(), port);
  std::string key(addr);
  
  //已存在，不用添加
  if(_weight.find(key) != _weight.end()) {
    return;
  }

  _weight[key] = _init_weight;
  _failed_count[key] = 0;
  _request_count[key] = 0;
  _first_failed_time[key] = -1;
  _latest_ajust_time[key] = -1;

  OcrAddress address;
  address.host = host;
  address.port = port;
  _address.push_back(address);
}

void WeightSelectAlgorithm::remove_addr(const std::string &host, int port) {
  AutoLock<Mutex> lock(&_mutex);
  char addr[32] = {'\0'};
  snprintf(addr, sizeof(addr) - 1, "%s:%d", host.c_str(), port);
  std::string key(addr);
  
  //不存在，不用移除
  if(_weight.find(key) == _weight.end()) {
    return;
  }
  
  _weight.erase(key);
  _failed_count.erase(key);
  _request_count.erase(key);
  _first_failed_time.erase(key);
  _latest_ajust_time.erase(key);

  OcrAddress address;
  address.host = host;
  address.port = port;
  std::vector<OcrAddress>::iterator it;
  for(it = _address.begin(); it != _address.end(); ++it) {
    if(it->host == host && it->port == port) {
      _address.erase(it);
      break;
    }
  }
}

void WeightSelectAlgorithm::failed(const std::string &host, int port) {
  AutoLock<Mutex> lock(&_mutex);
  char addr[32] = {'\0'};
  snprintf(addr, sizeof(addr) - 1, "%s:%d", host.c_str(), port);
  std::string key(addr);
  int sec = _get_time_sec();
  
  //被访问的时候该节点被移除
  if(_weight.find(key) == _weight.end()) {
    return;
  }
  
  //降权后在这段间隔内不降第二次，
  //防止由于同一时刻失败过多导致降权太厉害，
  //这段间隔的作用在于把此波高峰过滤掉
  int t = _latest_ajust_time[key];
  if(t != -1 && sec - t <= _ajust_interval) {
    LOG_INFO("%s latest ajust time=%d current time=%d then pass", addr, t, sec);
    return;
  }

  //第一次失败，或者某次降权后第一次失败
  if(_first_failed_time[key] == -1) {
    _first_failed_time[key] = sec;
  }

  //在规定的时间间隔内统计失败次数
  if(sec - _first_failed_time[key] <= _interval) {
    //失败次数大于上限值
    if(_failed_count[key] + 1 >= (_failed_times_bound * _weight[key] / _init_weight)) {
      _weight[key] -= _decrease_delta;
      if(_weight[key] < _weight_floor_bound) {
        _weight[key] = _weight_floor_bound;
      }
      //死节点
      if(_weight[key] == 0) {
        _dead_nodes.insert(key);
      }

      LOG_INFO("%s decrease weight:%d", addr, _weight[key]);
      _failed_count[key] = 0;
      _first_failed_time[key] = -1;
      _latest_ajust_time[key] = sec;
    } else {
      _failed_count[key] += 1;
    }
  } else {
    _first_failed_time[key] = sec;
    _failed_count[key] = 1;
  }
}

void WeightSelectAlgorithm::succeed(const std::string &host, int port) {
  AutoLock<Mutex> lock(&_mutex);
  char addr[32] = {'\0'};
  snprintf(addr, sizeof(addr) - 1, "%s:%d", host.c_str(), port);
  std::string key(addr);
  int sec = _get_time_sec();
  
  //被访问的时候该节点被移除
  if(_weight.find(key) == _weight.end()) {
    return;
  }

  int t = _latest_ajust_time[key];
  //服务一直运行良好，没有降权记录
  if(t == -1) {
    return;
  }
  
  //最近一次权重调整没有超过keep_time
  if(sec - t <= _keep_time) {
    return;
  }
  
  //尝试提升权重
  int w = _weight[key] += _increase_delta;
  
  if(w > _init_weight) {
    _weight[key] = _init_weight;
  } else {
    _weight[key] = w;
  }

  LOG_INFO("%s increase weight:%d", addr, _weight[key]);
  _latest_ajust_time[key] = sec;
}

void WeightSelectAlgorithm::_relive() {
  if(_dead_nodes.size() == 0) {
    return;
  }

  std::set<std::string> nodes;
  std::set<std::string>::iterator it;
  for(it = _dead_nodes.begin(); it != _dead_nodes.end(); ++it) {
    //节点死掉之后被移除了
    if(_weight.find(*it) == _weight.end()) {
      LOG_INFO("%s relive, but has been removed", it->c_str());
      continue;
    }

    int t = _get_time_sec();
    //静默期内不复活
    if(t - _latest_ajust_time[*it] <= _keep_time) {
      nodes.insert(*it);
      continue;
    }
    
    //死亡时间超过静默期，复活
    int w = _weight[*it] + _increase_delta;
    w = w > _init_weight ? _init_weight : w;
    _weight[*it] = w;
    _latest_ajust_time[*it] = t;
    LOG_INFO("%s relive, weight=%d", it->c_str(), w);
  }
  _dead_nodes.swap(nodes);
}

int WeightSelectAlgorithm::next(OcrAddress &ocr_addr) {
  AutoLock<Mutex> lock(&_mutex);
  int addr_num = _address.size();
  
  if(addr_num == 0) {
    return -1;
  }
  
  _relive();

  char addr[32];
  int retry = 0; 
  while(retry++ < addr_num) {
    memset(addr, 0x00, sizeof(addr));
    int step = abs(rand()) % _max_step + 1; //暂时先写死吧，只做个测试
    _index = (_index + step) % addr_num;
    snprintf(addr, sizeof(addr) - 1, "%s:%d", _address[_index].host.c_str(), _address[_index].port);
    std::string key(addr);
    int w = _weight[key];
    int r = _request_count[key];
    _request_count[key] = (r + 1) % _init_weight;
    if(_request_count[key] < w) {
      ocr_addr.host = _address[_index].host;
      ocr_addr.port = _address[_index].port;
      return 0;
    }
  } 

  return -1;
}


