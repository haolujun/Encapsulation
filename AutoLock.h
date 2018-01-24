
/***************************************************************************************
 *
 * Copyright (c) 2014 Lejent, Inc. All Rights Reserved
 *
 **************************************************************************************/
/**
 *    @file: AutoLock.h
 *  @author: zhangxingbiao(xingbiao.zhang@lejent.com)
 *    @date: 01/12/2017 04:12:23 PM
 * @version: 1.0 
 *   @brief: 
 *  
 **/
#ifndef __AUTO_LOCK_H__
#define __AUTO_LOCK_H__

template <class LOCK>
class AutoLock{
  public:
    AutoLock(LOCK *lock) {
      _lock = lock;
      _lock->lock();
    }
    ~AutoLock() {
      _lock->unlock();
    }
  private:
    LOCK *_lock;
};

#endif

