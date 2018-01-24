
/***************************************************************************************
 *
 * Copyright (c) 2016 Lejent, Inc. All Rights Reserved
 *
 **************************************************************************************/
/**
 *    @file: Timer.cpp
 *  @author: zhangxingbiao(xingbiao.zhang@lejent.com)
 *    @date: 09/28/2016 04:47:34 PM
 * @version: 1.0 
 *   @brief: 
 *  
 **/

#include "Timer.h"
void Timer::start(){
  gettimeofday(&_begin, 0x00);
}

void Timer::stop(){
  gettimeofday(&_end, 0x00);
}

long Timer::get_time_cost(){
  return _end.tv_sec * 1000 + _end.tv_usec / 1000 - _begin.tv_sec * 1000 - _begin.tv_usec / 1000;
}

long Timer::get_current_time() {
  struct timeval t;
  gettimeofday(&t, 0x00);
  return t.tv_sec * 1000 + t.tv_usec / 1000;
}
