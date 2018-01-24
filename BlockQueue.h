
/***************************************************************************************
 *
 * Copyright (c) 2014 Lejent, Inc. All Rights Reserved
 *
 **************************************************************************************/
/**
 *    @file: BlockQueue.h
 *  @author: zhangxingbiao(xingbiao.zhang@lejent.com)
 *    @date: 10/28/2016 10:39:19 AM
 * @version: 1.0 
 *   @brief: 
 *  
 **/
#ifndef __BLOCK_QUEUE_H__
#define __BLOCK_QUEUE_H__
#include <pthread.h>
#include <queue>

template <class EVENT>
class BlockQueue {
  public:
    BlockQueue(){
      pthread_mutex_init(&_mutex, NULL);
      pthread_cond_init(&_cond, NULL);
      _size = 0;
      _free = 0;
    }
    
    ~BlockQueue(){
      pthread_mutex_destroy(&_mutex);
      pthread_cond_destroy(&_cond);
    }

    bool empty(){
      return _size == 0 ? true : false;
    }

    EVENT get() {
      pthread_mutex_lock(&_mutex);
      _free++;
      while(empty()) {
        pthread_cond_wait(&_cond, &_mutex);
      } 
      EVENT e = _qu.front();
      _qu.pop();
      _free--;
      _size--;
      if(_size > 0 && _free > 0) {
        pthread_cond_signal(&_cond);
      }
      pthread_mutex_unlock(&_mutex);
      
      return e;
    }
    
    void put(EVENT e) {
      pthread_mutex_lock(&_mutex);
      _qu.push(e);
      _size++;
      if(_free > 0) {
        pthread_cond_signal(&_cond);
      }
      pthread_mutex_unlock(&_mutex);
    }
    
  private:
    std::queue<EVENT> _qu;
    pthread_cond_t _cond;
    pthread_mutex_t _mutex;
    volatile long _size;
    volatile int _free;
};

#endif

