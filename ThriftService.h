
/***************************************************************************************
 *
 * Copyright (c) 2014 Lejent, Inc. All Rights Reserved
 *
 **************************************************************************************/
/**
 *    @file: ThriftService.h
 *  @author: zhangxingbiao(xingbiao.zhang@lejent.com)
 *    @date: 10/25/2016 01:07:04 PM
 * @version: 1.0 
 *   @brief: 
 *  
 **/

#ifndef __THRIFT_SERVICE_H__
#define __THRIFT_SERVICE_H__

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PlatformThreadFactory.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/server/TNonblockingServer.h>
#include <thrift/server/TThreadedServer.h>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace apache::thrift::concurrency;

template <class SERVICE_IF, class SERVICE_PROCESSOR>
class ThriftService {
  public:
    ThriftService(std::string &host, unsigned short port, int send_timeout, int recv_timeout) {
      _host = host;
      _port = port;
      _send_timeout = send_timeout;
      _recv_timeout = recv_timeout;
    }
    ~ThriftService() {}
  
    void begin_threadpool_server(SERVICE_IF *handle, int thread_num) {
      boost::shared_ptr<SERVICE_IF> handler(handle);
	    boost::shared_ptr<TProcessor> processor(new SERVICE_PROCESSOR(handler));
	    boost::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
  	  boost::shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
      boost::shared_ptr<TServerTransport> serverTransport(new TServerSocket(_port, _send_timeout, _recv_timeout));
	    boost::shared_ptr<ThreadManager> threadManager = ThreadManager::newSimpleThreadManager(thread_num);
	    boost::shared_ptr<PosixThreadFactory> threadFactory = boost::shared_ptr<PosixThreadFactory>(new PosixThreadFactory()); 	
	    
      threadManager->threadFactory(threadFactory);
	    threadManager->start();
      /*
      ::apache::thrift::server::TThreadPoolServer server(
                          processor,
						              serverTransport,
						              transportFactory,
						              protocolFactory,
						              threadManager
                        );
      */
                     
      ::apache::thrift::server::TNonblockingServer server(
                          processor,
						              protocolFactory,
						              _port,
						              threadManager
                        );
      
	    server.serve();
    }

  private:
    std::string _host;
    unsigned short _port;
    int _send_timeout;
    int _recv_timeout;
};

#endif
