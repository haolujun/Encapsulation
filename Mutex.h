
/***************************************************************************************
 *
 * Copyright (c) 2014 Lejent, Inc. All Rights Reserved
 *
 **************************************************************************************/
/**
 *    @file: Mutex.h
 *  @author: zhangxingbiao(xingbiao.zhang@lejent.com)
 *    @date: 01/12/2017 04:00:39 PM
 * @version: 1.0 
 *   @brief: 
 *  
 **/

#ifndef __MUTEX_H__
#define __MUTEX_H__

class Mutex {
  public:
    Mutex() {
      pthread_mutex_init(&_mutex, NULL);
    }
    void lock() {
      pthread_mutex_lock(&_mutex);
    }
    void unlock() {
      pthread_mutex_unlock(&_mutex);
    }
  private:
    pthread_mutex_t _mutex;
};

#endif
