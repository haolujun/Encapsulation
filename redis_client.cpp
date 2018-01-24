#include "Logger.h"
#include "redis_client.h"
#include <iostream>
#include <cstdlib>

const int MAX_TRY_TIMES = 3;

int RedisClient::set(const std::string &key, const std::string &value) {
  if (key.empty() || value.empty()) {
    return -2;
  }
  redisReply *reply = (redisReply *)_real_set_one(key, value);

  //判断执行是否成功
  int ret = -1;
  if (reply != NULL && reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK") == 0) {
    ret = 0;
  }
  freeReplyObject(reply);
  return ret;
}

int RedisClient::get(const std::string &key, std::string &value) {
  if (key.empty()) {
    return -2;
  }

  redisReply *reply = (redisReply *)_real_get_one(key);
  //判断执行是否成功
  int ret = -1;
  if (reply->type == REDIS_REPLY_NIL) {
    //该key不存在
    ret = 1;
  } else if (reply->type == REDIS_REPLY_STRING) {
    //返回结果
    value.assign(reply->str, reply->len);
    ret = 0;
  }
  freeReplyObject(reply);
  return ret;
}

bool RedisNonClusterClient::init(const std::string &server_info_bak,
    const std::string &password, long timeout) {
  if (_redis_context != NULL) {
    redisFree(_redis_context);
    _redis_context = NULL;
  }
  
  _password = password;
  _server_info = server_info_bak;
  //ip:port:db 或者 ip:port
  std::string server_info(server_info_bak);

  int db = 0;
  int port = 0;
  size_t idx1 = server_info.find(':');
  if (idx1 == std::string::npos) {
    return false;
  }
  //ip\0port:db 或者 ip\0port
  server_info[idx1] = '\0';
  size_t idx2 = server_info.find(':', idx1 + 1);
  if (idx2 == std::string::npos) {
    //ip\0port
    db = 0;
  } else {
    //ip\0port\0db
    server_info[idx2] = '\0';
    db = atoi(server_info.c_str() + idx2 + 1);
  }
  port = atoi(server_info.c_str() + idx1 + 1);

  if (timeout > 0) {
    struct timeval tv;
    tv.tv_sec = timeout / 1000;
    tv.tv_usec = (timeout % 1000) * 1000;
    _redis_context = redisConnectWithTimeout(server_info.c_str(), port, tv);
  } else {
    _redis_context = redisConnect(server_info.c_str(), port);
  }

  if (_redis_context == NULL || _redis_context->err) {
    return false;
  }

  redisEnableKeepAlive(_redis_context);
  
  if (!password.empty()) {
    //需要密码验证
    redisReply *reply = (redisReply *)redisCommand(_redis_context,
        "auth %s", password.c_str());
    if (reply == NULL) {
      return false;
    }

    bool bval = false;
    if (reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK") == 0) {
      bval = true;
    }
    freeReplyObject(reply);
    if (!bval) {
      //密码验证失败
      return false;
    }
  }

  if (db != 0) {
    //切换到指定db
    redisReply *reply = (redisReply *)redisCommand(_redis_context, "select %d", db);
    if (reply == NULL) {
      return false;
    }

    bool bval = false;
    if (reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK") == 0) {
      bval = true;
    }
    freeReplyObject(reply);
    return bval;
  }

  return true;
}

int RedisNonClusterClient::mset(const std::vector<std::string> &keys,
    const std::vector<std::string> &values) {
  if (keys.size() != values.size() || keys.empty()) {
    return -2;
  }

  int argc = keys.size() * 2 + 1;
  const char **argv = new const char *[argc];
  size_t *argv_len = new size_t[argc];
  argv[0] = "MSET";
  argv_len[0] = 4;
  for (int i = 0, j = keys.size(), k = 1; i < j; ++i) {
    argv[k] = keys[i].c_str();
    argv_len[k] = keys[i].length();
    ++k;
    argv[k] = values[i].c_str();
    argv_len[k] = values[i].length();
    ++k;
  }
  redisReply *reply = (redisReply *)redisCommandArgv(_redis_context, argc, argv, argv_len);
  //判断执行是否成功
  int ret = -1;
  if (reply != NULL && reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK") == 0) {
    ret = 0;
  }

  freeReplyObject(reply);
  delete []argv;
  delete []argv_len;
  return ret;
}

int RedisNonClusterClient::mget(const std::vector<std::string> &keys,
    std::vector<std::string> &values) {
  if (keys.empty()) {
    return -2;
  }

  int argc = keys.size() + 1;
  const char **argv = new const char *[argc];
  size_t *argv_len = new size_t[argc];
  argv[0] = "MGET";
  argv_len[0] = 4;
  for (int i = 0, j = keys.size(), k = 1; i < j; ++i) {
    argv[k] = keys[i].c_str();
    argv_len[k] = keys[i].length();
    ++k;
  }
  redisReply *reply = (redisReply *)redisCommandArgv(_redis_context, argc, argv, argv_len);
  //判断执行是否成功
  int ret = -1;
  if (reply != NULL && reply->type == REDIS_REPLY_ARRAY) {
    redisReply *one_reply = NULL;
    for (size_t i = 0; i < reply->elements; ++i) {
      one_reply = reply->element[i];
      if (one_reply->type == REDIS_REPLY_NIL) {
        //key不存在
        values.push_back("");
      } else if (one_reply->type == REDIS_REPLY_STRING) {
        values.push_back(std::string(one_reply->str, one_reply->len));
      } else {
        //key对应的value不是字符串类型（这种情况基本上不会发生）
        values.push_back("");
      }
    }
    ret = 0;
  }
  freeReplyObject(reply);

  delete []argv;
  delete []argv_len;
  return ret;
}

int RedisNonClusterClient::get(const std::string &key, std::vector<unsigned char> &vec) {
  int ret = 0;
  redisReply *reply = NULL;
  redisAppendCommand(_redis_context, "GET %b", key.c_str(), key.size());
  ret = redisGetReply(_redis_context, (void **)&reply);
  
  if (ret == REDIS_ERR || reply == NULL) {
    ret = -1;
    if(reply != NULL) {
      freeReplyObject(reply);
      reply = NULL;
    }
    LOG_ERROR(debug_log, "get key=%s error, then retry", key.c_str());
    init(_server_info, _password);
    redisAppendCommand(_redis_context, "GET %b", key.c_str(), key.size());
    ret = redisGetReply(_redis_context, (void **)&reply);
    if(ret == REDIS_ERR || reply == NULL) {
      ret = -1;
      LOG_ERROR(debug_log, "get key=%s error and retry still failed!", key.c_str());
    } else {
      ret = 0;
      LOG_ERROR(debug_log, "get key=%s error and retry succeed!", key.c_str());
      if(reply->type == REDIS_REPLY_NIL) {
        LOG_ERROR(debug_log, "get key=%s not exist", key.c_str());
      } else {
        vec.resize(reply->len);
        for(int i = 0; i < reply->len; ++i) {
          vec[i] = reply->str[i];
        }
      }
    }
  } else {
    if(reply->type == REDIS_REPLY_NIL) {
      ret = 0;
      LOG_ERROR(debug_log, "get key=%s not exist", key.c_str());
    } else {
      vec.resize(reply->len);
      for(int i = 0; i < reply->len; ++i) {
        vec[i] = reply->str[i];
      }
      ret = 0;
    }
  }

  if(reply != NULL) {
    freeReplyObject(reply);
  }
  return ret;
}

bool RedisClusterClient::init(const std::string &server_info,
    const std::string &password, long timeout) {
  if (_redis_context != NULL) {
    redisClusterFree(_redis_context);
    _redis_context = NULL;
  }

  _server_info = server_info;

  if (timeout > 0) {
    struct timeval tv;
    tv.tv_sec = timeout / 1000;
    tv.tv_usec = (timeout % 1000) * 1000;
    _redis_context = redisClusterConnectWithTimeout(server_info.c_str(), tv, HIRCLUSTER_FLAG_NULL);
  } else {
    _redis_context = redisClusterConnect(server_info.c_str(), HIRCLUSTER_FLAG_NULL);
  }

  if (_redis_context == NULL || _redis_context->err) {
    return false;
  }

  return true;
}

int RedisClusterClient::mset(const std::vector<std::string> &keys,
    const std::vector<std::string> &values) {
  if (keys.empty() || keys.size() != values.size()) {
    return -2;
  }

  for (int i = 0, j = keys.size(); i < j; ++i) {
    redisClusterAppendCommand(_redis_context, "SET %b %b", keys[i].c_str(), keys[i].length(),
        values[i].c_str(), values[i].length());
  }

  //记录请求失败的keys
  std::vector<int> failed_keys;
  redisReply *reply = NULL;
  int ret = 0;
  for (int i = 0, j = keys.size(); i < j; ++i) {
    reply = NULL;
    ret = redisClusterGetReply(_redis_context, (void **)&reply);
    if (ret == REDIS_ERR || reply == NULL
        || reply->type != REDIS_REPLY_STATUS || strcmp(reply->str, "OK") != 0) {
      failed_keys.push_back(i);
    }
    freeReplyObject(reply);
  }

  redisClusterReset(_redis_context);

  if (failed_keys.empty()) {
    return 0;
  }

  int try_times = 1;
  int failed_size = 0;

retry:

  failed_size = failed_keys.size();
  for (int i = 0; i < failed_size; ++i) {
    redisClusterAppendCommand(_redis_context, "SET %b %b",
        keys[failed_keys[i]].c_str(), keys[failed_keys[i]].length(),
        values[failed_keys[i]].c_str(), values[failed_keys[i]].length());
  }

  failed_keys.clear();
  for (int i = 0; i < failed_size; ++i) {
    reply = NULL;
    ret = redisClusterGetReply(_redis_context, (void **)&reply);
    if (ret == REDIS_ERR || reply == NULL
        || reply->type != REDIS_REPLY_STATUS || strcmp(reply->str, "OK") != 0) {
      //failed
      failed_keys.push_back(i);
    }
    freeReplyObject(reply);
  }

  redisClusterReset(_redis_context);
  ++try_times;

  if (failed_keys.empty()) {
    return 0;
  } else if (try_times < MAX_TRY_TIMES) {
    goto retry;
  } else {
    return -3;
  }
}

int RedisClusterClient::mget(const std::vector<std::string> &keys,
    std::vector<std::string> &values) {
  if (keys.empty()) {
    return 0;
  }

  for (int i = 0, j = keys.size(); i < j; ++i) {
    redisClusterAppendCommand(_redis_context, "GET %b", keys[i].c_str(), keys[i].length());
  }

  //记录请求失败的keys
  std::vector<int> failed_keys;
  redisReply *reply = NULL;
  int ret = 0;
  for (int i = 0, j = keys.size(); i < j; ++i) {
    reply = NULL;
    ret = redisClusterGetReply(_redis_context, (void **)&reply);
    if (ret == REDIS_ERR || reply == NULL) {
      failed_keys.push_back(i);
      values.push_back("");
    } else if (reply->type == REDIS_REPLY_STRING) {
      //获取到该key的value
      values.push_back(std::string(reply->str, reply->len));
    } else if (reply->type == REDIS_REPLY_NIL) {
      //该key不存在
      values.push_back("");
    } else {
      failed_keys.push_back(i);
      values.push_back("");
    }
    freeReplyObject(reply);
  }

  redisClusterReset(_redis_context);

  if (failed_keys.empty()) {
    return 0;
  }

  std::vector<int> failed_keys2;
  int try_times = 1;
  int failed_size = 0;

retry:

  //前面失败的value都已经设置为""
  failed_size = failed_keys.size();
  for (int i = 0; i < failed_size; ++i) {
    redisClusterAppendCommand(_redis_context, "GET %b",
        keys[failed_keys[i]].c_str(), keys[failed_keys[i]].length());
  }

  failed_keys2.clear();
  for (int i = 0; i < failed_size; ++i) {
    reply = NULL;
    ret = redisClusterGetReply(_redis_context, (void **)&reply);
    if (ret == REDIS_ERR || reply == NULL) {
      failed_keys2.push_back(i);
    } else if (reply->type == REDIS_REPLY_STRING) {
      values[failed_keys[i]].assign(reply->str, reply->len);
    } else if (reply->type != REDIS_REPLY_NIL) {
      //不是REDIS_REPLY_STRING 、不是REDIS_REPLY_NIL
      failed_keys2.push_back(i);
    }
    freeReplyObject(reply);
  }

  redisClusterReset(_redis_context);
  ++try_times;

  if (failed_keys2.empty()) {
    return 0;
  } else if (try_times < MAX_TRY_TIMES) {
    failed_keys.swap(failed_keys2);
    goto retry;
  } else {
    return -3;
  }
}

int RedisNonClusterClient::hset(const std::string &key, const std::string &h_key, const std::string &h_value) {
  int ret = 0;
  redisReply *reply = NULL;
  redisAppendCommand(_redis_context, "HSET %b %b %b", key.c_str(), key.size(), h_key.c_str(), h_key.size(), h_value.c_str(), h_value.size());
  ret = redisGetReply(_redis_context, (void **)&reply);

  if (ret == REDIS_ERR || reply == NULL) {
    if(reply != NULL) {
      freeReplyObject(reply);
      reply = NULL;
    }
    init(_server_info, _password);
    redisAppendCommand(_redis_context, "HSET %b %b %b", key.c_str(), key.size(), h_key.c_str(), h_key.size(), h_value.c_str(), h_value.size());
    ret = redisGetReply(_redis_context, (void **)&reply);
    LOG_ERROR(debug_log, "redis: set key=%s error then retry", key.c_str());
    if(ret == REDIS_ERR || reply == NULL) {
      ret = -1;
      LOG_ERROR(debug_log, "redis: set key=%s error and retry failed", key.c_str());
    } else {
      LOG_ERROR(debug_log, "redis: set key=%s error and retry succeed", key.c_str());
    }
  } else {
    ret = 0;
  }
  
  if(reply != NULL) {
    freeReplyObject(reply);
  }
  return ret;
}

int RedisNonClusterClient::hmset(const std::string &key,
    const std::vector<std::string> &fields, const std::vector<std::string> &values) {
  int ret = _hmset(key, fields, values);
  if(ret == -1) {
    LOG_ERROR(debug_log, "redis: hmset key=%s error and then retry", key.c_str());
    init(_server_info, _password);
    ret = _hmset(key, fields, values);
    if(ret == -1) {
      LOG_ERROR(debug_log, "redis: hmset key=%s error and retry failed", key.c_str());
    } else {
      LOG_ERROR(debug_log, "redis: hmset key=%s error and retry succeed", key.c_str());
    }
  }
} 

int RedisNonClusterClient::_hmset(const std::string &key,
    const std::vector<std::string> &fields, const std::vector<std::string> &values) {
  if (key.empty() || fields.empty() || fields.size() != values.size()) {
    return -2;
  }

  int argc = fields.size() * 2 + 2;
  if (argc > MAX_ARGV_SIZE) {
    return -4;
  }

  const char *argv[MAX_ARGV_SIZE];
  size_t argv_len[MAX_ARGV_SIZE];
  argv[0] = "HMSET";
  argv_len[0] = 5;
  argv[1] = key.c_str();
  argv_len[1] = key.length();
  for (int i = 0, j = fields.size(), k = 2; i < j; ++i) {
    argv[k] = fields[i].c_str();
    argv_len[k] = fields[i].length();
    ++k;
    argv[k] = values[i].c_str();
    argv_len[k] = values[i].length();
    ++k;
  }
  redisReply *reply = (redisReply *)redisCommandArgv(_redis_context, argc, argv, argv_len);
  //判断执行是否成功
  int ret = -1;
  if (reply != NULL && reply->type == REDIS_REPLY_STATUS) {
    if(reply->str == NULL) {
      ret = -1;
    } else {
      ret = (strcmp(reply->str, "OK") == 0) ? 0 : -1;
    }
  }
  freeReplyObject(reply);
  return ret;
}

int RedisNonClusterClient::del(const std::vector<std::string> &keys) {
  if (keys.empty()) {
    return -2;
  }

  int argc = keys.size() + 1;
  if (argc > MAX_ARGV_SIZE) {
    return -4;
  }

  const char *argv[MAX_ARGV_SIZE];
  size_t argv_len[MAX_ARGV_SIZE];
  argv[0] = "DEL";
  argv_len[0] = 3;
  for (int i = 0, j = keys.size(), k = 1; i < j; ++i, ++k) {
    argv[k] = keys[i].c_str();
    argv_len[k] = keys[i].length();
  }
  redisReply *reply = (redisReply *)redisCommandArgv(_redis_context, argc, argv, argv_len);
  //判断执行是否成功
  int ret = -1;
  if (reply != NULL && reply->type == REDIS_REPLY_INTEGER) {
    ret = 0;
  }

  freeReplyObject(reply);
  return ret;
}

int RedisNonClusterClient::hget(const std::string &key, const std::string &h_key, std::string &h_value) {
  int ret = 0;
  redisReply *reply = NULL;
  redisAppendCommand(_redis_context, "HGET %b %b", key.c_str(), key.size(), h_key.c_str(), h_key.size());
  ret = redisGetReply(_redis_context, (void **)&reply);
  if (ret == REDIS_ERR || reply == NULL) {
    if(reply != NULL) {
      freeReplyObject(reply);
      reply = NULL;
    }
    init(_server_info, _password);
    redisAppendCommand(_redis_context, "HGET %b %b", key.c_str(), key.size(), h_key.c_str(), h_key.size());
    ret = redisGetReply(_redis_context, (void **)&reply);
    if(ret == REDIS_ERR || reply == NULL) {
      ret = -1;
      LOG_ERROR(debug_log, "redis: set key=%s error and retry failed", key.c_str());
    } else {
      ret = 0;
      LOG_ERROR(debug_log, "redis: set key=%s error and retry succeed", key.c_str());
    }
  } else {
    ret = 0;
  }
  
  if(ret == 0) {
    if(reply->str != NULL) {
      h_value = std::string(reply->str);
    }
  }

  if(reply != NULL) {
    freeReplyObject(reply);
  }
  return ret;

}

int RedisClusterClient::hset(const std::string &key, const std::string &h_key, const std::string &h_value) {
  int ret = 0;
  redisReply *reply = NULL;
  redisClusterAppendCommand(_redis_context, "HSET %b %b %b", key.c_str(), key.size(), h_key.c_str(), h_key.size(), h_value.c_str(), h_value.size());
  ret = redisClusterGetReply(_redis_context, (void **)&reply);

  if (ret == REDIS_ERR || reply == NULL) {
    if(reply != NULL) {
      freeReplyObject(reply);
      reply = NULL;
    }
    redisClusterReset(_redis_context);
    init(_server_info);
    redisClusterAppendCommand(_redis_context, "HSET %b %b %b", key.c_str(), key.size(), h_key.c_str(), h_key.size(), h_value.c_str(), h_value.size());
    ret = redisClusterGetReply(_redis_context, (void **)&reply);
    LOG_ERROR(debug_log, "redis: set key=%s error then retry", key.c_str());
    if(ret == REDIS_ERR || reply == NULL) {
      ret = -1;
      LOG_ERROR(debug_log, "redis: set key=%s error and retry failed", key.c_str());
    } else {
      LOG_ERROR(debug_log, "redis: set key=%s error and retry succeed", key.c_str());
    }
  } else {
    ret = 0;
  }
  
  if(reply != NULL) {
    freeReplyObject(reply);
  }
  redisClusterReset(_redis_context);
  return ret;
}

int RedisClusterClient::get(const std::string &key, std::vector<unsigned char> &vec) {
  int ret = 0;
  redisReply *reply = NULL;
  redisClusterAppendCommand(_redis_context, "GET %b", key.c_str(), key.size());
  ret = redisClusterGetReply(_redis_context, (void **)&reply);
  
  if (ret == REDIS_ERR || reply == NULL) {
    LOG_ERROR(debug_log, "get key=%s error then retry", key.c_str());
    ret = -1;
    if(reply != NULL) {
      freeReplyObject(reply);
      reply = NULL;
    }
    redisClusterReset(_redis_context);
    init(_server_info);
    redisClusterAppendCommand(_redis_context, "GET %b", key.c_str(), key.size());
    ret = redisClusterGetReply(_redis_context, (void **)&reply);
    if(ret == REDIS_ERR || reply == NULL) {
      ret = -1;
      LOG_ERROR(debug_log, "get key=%s error and retry still failed!", key.c_str());
    }
  } else {
    vec.resize(reply->len);
    for(int i = 0; i < reply->len; ++i) {
      vec[i] = reply->str[i];
    }
    ret = reply->len;
  }

  if(reply != NULL) {
    freeReplyObject(reply);
  }
  
  redisClusterReset(_redis_context);
  return ret;
}


