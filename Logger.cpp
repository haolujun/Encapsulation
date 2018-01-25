
/***************************************************************************************
 *
 * Copyright (c) 2017 Lejent, Inc. All Rights Reserved
 *
 **************************************************************************************/
/**
 *    @file: Logger.cpp
 *  @author: zhangxingbiao(xingbiao.zhang@lejent.com)
 *    @date: 02/14/2017 11:44:10 AM
 * @version: 1.0 
 *   @brief: 
 *  
 **/

#include "Logger.h"
#include <stdio.h>
#include <stdarg.h>
#include <string>

void LOG_INFO(const char *format, ...) {
  char log[256 * 1024];
  va_list st;
  va_start(st, format);
  vsnprintf(log, sizeof(log), format, st);
  va_end(st);
  printf("%s\n", log);
}

void LOG_ERROR(log4cxx::LoggerPtr logger, const char *format, ...) {
  char log[256 * 1024];
  va_list st;
  va_start(st, format);
  vsnprintf(log, sizeof(log), format, st);
  va_end(st);
  printf("%s\n", log);
}

void LOG_FATAL(log4cxx::LoggerPtr logger, const char *format, ...) {
  char log[256 * 1024];
  va_list st;
  va_start(st, format);
  vsnprintf(log, sizeof(log), format, st);
  va_end(st);
  printf("%s\n", log);
}


