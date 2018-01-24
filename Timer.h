
/***************************************************************************************
 *
 * Copyright (c) 2014 Lejent, Inc. All Rights Reserved
 *
 **************************************************************************************/
/**
 *    @file: Timer.h
 *  @author: zhangxingbiao(xingbiao.zhang@lejent.com)
 *    @date: 09/28/2016 04:44:47 PM
 * @version: 1.0 
 *   @brief: 
 *  
 **/

#ifndef __TIMER_H__
#define __TIMER_H__
#include <sys/time.h>
class Timer {
  public:
    Timer(){
    }
    void start();
    void stop();
    long  get_time_cost();
    long  get_current_time();
  private:
    struct timeval _begin;
    struct timeval _end;
};
#endif

