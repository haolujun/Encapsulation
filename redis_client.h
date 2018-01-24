
#ifndef AFANTI_REDIS_TOOL_H_
#define AFANTI_REDIS_TOOL_H_

#include <hircluster.h>
#include <string>
#include <vector>
#include <cstring>

#define MAX_ARGV_SIZE 1024

/**
 * RedisClient
 * @note 线程不安全
 **/
class RedisClient {

public:

  RedisClient() {}

  virtual ~RedisClient() {}

  /**
   * @brief 初始化方法，建立与server的连接，虚函数，需要子类具体实现
   * @param [in]  server_info，字符串
   * @param [in]  password
   * @param [in]  timeout，连接server的超时时间（单位ms）：<=0，无超时判断
   * @return bool
   **/
  virtual bool init(const std::string &server_info,
      const std::string &password = "", long timeout = 0) = 0;

  /**
   * @brief key-value单条写入方法
   * @param [in]  key
   * @param [in]  value
   * @return int
   *  0：命令执行成功
   *  -1：redis-server执行命令失败，
   *      此时与redis-server的连接失败，需要重新init
   *  -2：输入的key/value问题
   **/
  int set(const std::string &key, const std::string &value);

  /**
   * @brief key-value批量写入方法
   * @param [in]  keys
   * @param [in]  values
   * @return int
   *  0：命令执行成功
   *  -1：redis-server执行命令失败，
   *      此时与redis-server的连接失败，需要重新init
   *  -2：输入的key/value问题
   *  -3：批量操作存在个别失败的情况
   **/
  virtual int mset(const std::vector<std::string> &keys,
      const std::vector<std::string> &values) = 0;

  /**
   * @brief key-value单条读取方法
   * @param [in]  key
   * @param [in]  value
   * @return int
   *  0：命令执行成功
   *  1：该key不存在
   *  -1：redis-server执行命令失败，
   *      此时与redis-server的连接失败，需要重新init
   *  -2：输入的key/value问题
   **/
  int get(const std::string &key, std::string &value);

  /**
   * @brief key-value批量读取方法，key不存在or读取失败的value设置为""
   * @param [in]  keys
   * @param [in]  values
   * @return int
   *  0：命令执行成功
   *  -1：redis-server执行命令失败，
   *      此时与redis-server的连接失败，需要重新init
   *  -2：输入的key/value问题
   *  -3：批量操作存在个别失败的情况（key不存在不认为是失败）
   **/
  virtual int mget(const std::vector<std::string> &keys,
      std::vector<std::string> &values) = 0;

  
  virtual int hmset(const std::string &key, const std::string &h_key, const std::string &h_value){}
  virtual int hget(const std::string &key, const std::string &h_key, std::string &h_value){}
  
  /**
   * @brief 获取操作失败的信息
   * @param [in]  keys
   * @param [in]  values
   * @return const char*
   **/
  virtual const char* get_error_info() {
    return NULL;
  }

private:

  /**
   * @brief key单条写入真正执行的方法，虚函数，需要子类实现
   * @param [in]  key
   * @param [in]  value
   * @return void*
   **/
  virtual void* _real_set_one(const std::string &key, const std::string &value) = 0;

  /**
   * @brief key单条读取真正执行的方法，虚函数，需要子类实现
   * @param [in]  key
   * @param [in]  value
   * @return void*
   **/
  virtual void* _real_get_one(const std::string &key) = 0;

};

/**
 * RedisNonClusterClient
 **/
class RedisNonClusterClient : public RedisClient {

public:

  RedisNonClusterClient() : _redis_context(NULL) {}

  virtual ~RedisNonClusterClient() {
    if (_redis_context != NULL) {
      redisFree(_redis_context);
      _redis_context = NULL;
    }
  }

  /**
   * @brief 初始化方法，建立与server的连接
   * @param [in]  server_info，字符串格式 ip:port:db or ip:port（此时默认db为0）
   * @param [in]  password
   * @param [in]  timeout，连接server的超时时间（单位ms）：<=0，无超时判断
   * @return bool
   **/
  bool init(const std::string &server_info, const std::string &password = "", long timeout = 0);

  /**
   * @brief 实现父类虚函数
   **/
  int mset(const std::vector<std::string> &keys, const std::vector<std::string> &values);

  /**
   * @brief 实现父类虚函数
   **/
  int mget(const std::vector<std::string> &keys, std::vector<std::string> &values);

  int hset(const std::string &key, const std::string &h_key, const std::string &h_value);
  
  int hget(const std::string &key, const std::string &h_key, std::string &h_value); 
  
  int hmset(
    const std::string &key,
    const std::vector<std::string> &fields,
    const std::vector<std::string> &values
  );

  int get(const std::string &key, std::vector<unsigned char> &vec);
  
  int del(const std::vector<std::string> &keys);
  /**
   * @brief 返回redis db中key的数量
   * @return int
   *  >=0：命令执行成功，返回key的数量
   *  -1：redis-server执行命令失败，
   *      此时与redis-server的连接失败，需要重新init
   **/
  long long dbsize() {
    redisReply *reply = (redisReply *)redisCommand(_redis_context, "DBSIZE");
    long long ret = -1;
    if (reply != NULL && reply->type == REDIS_REPLY_INTEGER) {
      ret = reply->integer;
    }
    freeReplyObject(reply);
    return ret;
  }

  /**
   * @brief 清空redis db中key的数据
   * @return int
   *  0：命令执行成功
   *  -1：redis-server执行命令失败，
   *      此时与redis-server的连接失败，需要重新init
   **/
  int flushdb() {
    redisReply *reply = (redisReply *)redisCommand(_redis_context, "FLUSHDB");
    int ret = -1;
    if (reply != NULL && reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK") == 0) {
      ret = 0;
    }
    freeReplyObject(reply);
    return ret;
  }

  /**
   * @brief 实现父类虚函数
   **/
  virtual const char* get_error_info() {
    if (_redis_context != NULL && _redis_context->err) {
      return _redis_context->errstr;
    }
    return NULL;
  }

private:

  int _hmset(
    const std::string &key,
    const std::vector<std::string> &fields,
    const std::vector<std::string> &values
  );

  void* _real_set_one(const std::string &key, const std::string &value) {
    return redisCommand(_redis_context, "SET %b %b",
        key.c_str(), key.length(), value.c_str(), value.length());
  }

  void* _real_get_one(const std::string &key) {
    return redisCommand(_redis_context, "GET %b", key.c_str(), key.length());
  }

  //不允许拷贝和赋值操作
  RedisNonClusterClient(const RedisNonClusterClient &other);
  RedisNonClusterClient& operator= (const RedisNonClusterClient &other);

private:

  redisContext *_redis_context;
  std::string _server_info;
  std::string _password;
};

class RedisClusterClient : public RedisClient {

public:

  RedisClusterClient() : _redis_context(NULL) {}

  virtual ~RedisClusterClient() {
    if (_redis_context != NULL) {
      redisClusterFree(_redis_context);
      _redis_context = NULL;
    }
  }

  /**
   * @brief 初始化方法，建立与server的连接
   * @param [in]  server_info，字符串格式 ip0:port0,ip1:port1,...
   * @param [in]  password
   * @param [in]  timeout，连接server的超时时间（单位ms）：<=0，无超时判断
   * @return bool
   **/
  bool init(const std::string &server_info, const std::string &password = "", long timeout = 0);

  /**
   * @brief 实现父类虚函数
   **/
  int mset(const std::vector<std::string> &keys, const std::vector<std::string> &values);

  /**
   * @brief 实现父类虚函数
   **/
  int mget(const std::vector<std::string> &keys, std::vector<std::string> &values);

  int hset(const std::string &key, const std::string &h_key, const std::string &h_value);
  
  int get(const std::string &key, std::vector<unsigned char> &vec);
  /**
   * @brief 实现父类虚函数
   **/
  virtual const char* get_error_info() {
    if (_redis_context != NULL && _redis_context->err) {
      return _redis_context->errstr;
    }
    return NULL;
  }

private:

  void* _real_set_one(const std::string &key, const std::string &value) {
    return redisClusterCommand(_redis_context, "set %b %b",
      key.c_str(), key.length(), value.c_str(), value.length());
  }

  void* _real_get_one(const std::string &key) {
    return redisClusterCommand(_redis_context, "get %b", key.c_str(), key.length());
  }

  //不允许拷贝和赋值操作
  RedisClusterClient(const RedisClusterClient &other);
  RedisClusterClient& operator= (const RedisClusterClient &other);

private:

  redisClusterContext *_redis_context;
  std::string _server_info;
};

#endif

