
/***************************************************************************************
 *
 * Copyright (c) 2014 Lejent, Inc. All Rights Reserved
 *
 **************************************************************************************/
/**
 *    @file: SelectAlgorithm.h
 *  @author: zhangxingbiao(xingbiao.zhang@lejent.com)
 *    @date: 01/25/2018 10:28:44 AM
 * @version: 1.0 
 *   @brief: 
 *  
 **/

#ifndef __SELECT_ALGORITHM_H__
#define __SELECT_ALGORITHM_H__

#include <string>
#include <vector>
#include <map>
#include <set>
#include <cstdio>
#include <sys/time.h>
#include <cstdlib>

#include "AutoLock.h"
#include "Mutex.h"

struct OcrAddress
{
	std::string host;
	int port;
};

class ServerWatcher;

class SelectAlgorithm {
  public:
    virtual void add_addr(const std::string &host, int port) = 0;
    virtual void remove_addr(const std::string &host, int port) = 0;
    virtual void failed(const std::string &host, int port) = 0;
    virtual void succeed(const std::string &host, int port) = 0;
    virtual int next(OcrAddress &addr) = 0; 
    virtual std::vector<OcrAddress> get() = 0;
};

class SimpleSelectAlgorithm : public SelectAlgorithm {
  public:
    SimpleSelectAlgorithm() {
      _index = 0;
      srand(unsigned(time(NULL)));
    }

    void add_addr(const std::string &host, int port) {
      AutoLock<Mutex> lock(&_mutex);
      char addr[32] = {'\0'};
      snprintf(addr, sizeof(addr) - 1, "%s:%d", host.c_str(), port);

      if(_addr_set.find(std::string(addr)) == _addr_set.end()) {
        OcrAddress ocr_addr;
        ocr_addr.host = host;
        ocr_addr.port = port;
        _address.push_back(ocr_addr);
        _addr_set.insert(std::string(addr));
      }
    }

    void remove_addr(const std::string &host, int port) {
      AutoLock<Mutex> lock(&_mutex);
      char addr[32] = {'\0'};
      snprintf(addr, sizeof(addr) - 1, "%s:%d", host.c_str(), port);
      
      if(_addr_set.find(std::string(addr)) == _addr_set.end()) {
        return;
      }

      _addr_set.erase(std::string(addr));
      std::vector<OcrAddress>::iterator it;
      for(it = _address.begin(); it != _address.end(); ++it) {
        if(it->host == host && it->port == port) {
          _address.erase(it);
          break;
        }
      }
    }

    void succeed(const std::string &host, int port) {}

    void failed(const std::string &host, int port) {}

    int next(OcrAddress &addr) {
      AutoLock<Mutex> lock(&_mutex);
      int num = _address.size();
      if(num == 0) { return -1; }
      
      _index = (_index + 1) % num;
      addr = _address[_index];
      return 0;
    }

    std::vector<OcrAddress> get() {
      AutoLock<Mutex> lock(&_mutex);
      return _address;
    }

  private:
    std::vector<OcrAddress> _address;
    std::set<std::string> _addr_set;
    Mutex _mutex;
    int _index;
};

class WeightSelectAlgorithm : public SelectAlgorithm{
  public:
    WeightSelectAlgorithm(
      int interval,
      int failed_times_bound,
      int weight_floor_bound,
      int keep_time,
      int ajust_interval,
      int decrease_delta,
      int increase_delta,
      int w,
      int max_step) {
      _interval = interval;
      _failed_times_bound = failed_times_bound;
      _weight_floor_bound = weight_floor_bound;
      _keep_time = keep_time;
      _ajust_interval = ajust_interval;
      _decrease_delta = decrease_delta;
      _increase_delta = increase_delta;
      _init_weight = w;
      _max_step = max_step;
      _index = 0;
      srand(unsigned(time(NULL)));
    }

    void add_addr(const std::string &host, int port);

    void remove_addr(const std::string &host, int port); 

    void failed(const std::string &host, int port);

    void succeed(const std::string &host, int port);

    int next(OcrAddress &addr);

    std::vector<OcrAddress> get() {
      AutoLock<Mutex> lock(&_mutex);
      return _address;
    }

  private:
    int _get_time_sec(){
      struct timeval t;
      gettimeofday(&t, NULL);
      return t.tv_sec;
    }
    
    void _relive();

    //间隔时间，统计这个时间间隔内的失败次数
    int _interval;
    //在时间间隔内，最大失败次数，超过这个次数就要减权重
    int _failed_times_bound;
    //权重下限
    int _weight_floor_bound;
    //升权或者降权后至少保持keep_time秒，在这期间可以再次降权，但是不能升权
    int _keep_time;
    //每次降权幅度
    int _decrease_delta;
    //每次升权幅度
    int _increase_delta;
    //每个节点初始权重
    int _init_weight;
    //降权间隔，降权后等待这个间隔再降下一次
    int _ajust_interval;
    //最大跳跃间隔
    int _max_step;

    std::set<std::string> _dead_nodes;
    std::vector<OcrAddress> _address;
    std::map<std::string, int> _weight;            //默认权重为10
    std::map<std::string, int> _failed_count;      //超时次数
    std::map<std::string, int> _request_count;     //接受的请求数
    std::map<std::string, int> _first_failed_time; //第一次请求失败的时间
    std::map<std::string, int> _latest_ajust_time; //最近一次调整权重的时间
    Mutex _mutex;
    int _index;
};

#endif
