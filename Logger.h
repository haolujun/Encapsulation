
/***************************************************************************************
 *
 * Copyright (c) 2014 Lejent, Inc. All Rights Reserved
 *
 **************************************************************************************/
/**
 *    @file: Logger.h
 *  @author: zhangxingbiao(xingbiao.zhang@lejent.com)
 *    @date: 02/14/2017 10:49:18 AM
 * @version: 1.0 
 *   @brief: 
 *  
 **/

#ifndef __LOGGER_H__
#define __LOGGER_H__
#include <stdio.h>
#include <stdarg.h>
#include <string>
#include <log4cxx/logger.h>
#include <iostream>
static log4cxx::LoggerPtr handle_log = log4cxx::Logger::getLogger("handle");
static log4cxx::LoggerPtr debug_log = log4cxx::Logger::getLogger("debug");
static log4cxx::LoggerPtr business_log = log4cxx::Logger::getLogger("business");

#define LOG_INFO(logger, format, arg...) \
do {\
  char *tmp_log_buf = new char[1024]; \
  snprintf(tmp_log_buf, 1024, format, ##arg);\
  LOG4CXX_INFO(logger, std::string(tmp_log_buf)); \
  delete [] tmp_log_buf; \
} while (0)

#define LOG_ERROR(logger, format, arg...) \
do {\
  char *tmp_log_buf = new char[1024]; \
  snprintf(tmp_log_buf, 1024, format, ##arg);\
  LOG4CXX_ERROR(logger, std::string(tmp_log_buf)); \
  delete [] tmp_log_buf; \
} while (0)

#define LOG_FATAL(logger, format, arg...) \
do {\
  char *tmp_log_buf = new char[1024]; \
  snprintf(tmp_log_buf, 1024, format, ##arg);\
  LOG4CXX_FATAL(logger, std::string(tmp_log_buf)); \
  delete [] tmp_log_buf; \
} while (0)

//void LOG_INFO(log4cxx::LoggerPtr logger, const char *format, ...);

//void LOG_ERROR(log4cxx::LoggerPtr logger, const char *format, ...);

//void LOG_FATAL(log4cxx::LoggerPtr logger, const char *format, ...);

#endif
