#ifndef ETCD_H_
#define ETCD_H_

#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <set>
#include <vector>
#include <string>
#include <map>
#include <iostream>
#include <pthread.h>
#include <curl/curl.h>  
#include <json/json.h>  
#include "Mutex.h"
#include "AutoLock.h"

const int MAX_ADDR_LEN = 512;
const int MAX_RETRY_TIMES = 10;

class Watcher{
  public:
    virtual void handle(const std::string &key, const std::string &value) = 0;
};

class CurlHandleWrapper {
    public:
      CurlHandleWrapper(CURL *easy_handle) {
        _easy_handle = easy_handle;
      }
      
      ~CurlHandleWrapper() {
        curl_easy_cleanup(_easy_handle);
      }

    private:
      CURL* _easy_handle;
};

class ETCD_Exception : public std::exception {
  public:
    ETCD_Exception() throw() {}
    ~ETCD_Exception() throw() {} 
    ETCD_Exception(const char *message) {
      this->message = std::string(message);
    }

    ETCD_Exception(std::string message) {
      this->message = message;
    }
    
    const char* what() {
      return message.c_str();
    }

  private:
    std::string message;
};

class ETCD {
  public:

    static ETCD* get_instance() {
      if(_cluster == NULL) {
        {
          AutoLock<Mutex> lock(&_instance_mutex);
          if(_cluster == NULL) {
            _cluster = new ETCD();
          }
        }
      }
      return _cluster;
    }
    //初始化需要一些种子节点
    
    static int init(const std::string &seeds, const std::string &username = "", const std::string &password = "");

    static void start_update() {
      pthread_create(&_update_thread_pid, NULL, ETCD::update, ETCD::get_instance());
    }

    static int release() {
      //pthread_join(_update_thread_pid, NULL);
      delete _cluster; 
    }

    int get(const std::string &key, std::string &value);
    
    int get(const std::string &key, std::map<std::string, std::string> &v_map);
    
    int set(const std::string &key, const std::string &value);

    int compare_and_set(const std::string &key, const std::string &prev_modify_index, const std::string &new_value);
    
    static std::string choose() {
      AutoLock<Mutex> lock(&_choose_mutex);
      _index = (_index + 1) % _client.size();
      return _client[_index];
    }

    static void reset(std::vector<std::string> &seeds) {
      AutoLock<Mutex> lock(&_choose_mutex);
      _client = seeds;
    }

    static CURL* make_curl_handle() { 
      CURL *handle = curl_easy_init();  
      if(handle == NULL) {
        throw new ETCD_Exception("curl_easy_init failed!");
      }
      return handle;
    }

  private:

    ETCD() {}

    ~ETCD() {
      curl_global_cleanup();
    }

    int _set(const std::string &addr, const std::string &key, const std::string &value);

    int _get(const std::string &addr, const std::string &key, std::map<std::string, std::string> &v_map);
    
    int _compare_and_set(const std::string &addr, const std::string &key, const std::string &prev_modify_index, const std::string &new_value);
    
    static int _get_all_members(std::vector<std::string> &clients);

    static pthread_t _update_thread_pid;

    static std::vector<std::string> _client;
    
    static int _index;

    static void* watch(void *param);

    static ETCD *_cluster;
    
    static Mutex _instance_mutex;
    
    static Mutex _choose_mutex;

    static Mutex _op_mutex;

    static void* update(void *param);
  
    static std::string _username;
    static std::string _password;
};

size_t collect_data(char *buffer, size_t size, size_t nmemb, void *user_p);
int process_data(const std::string &input, std::map<std::string, std::string> &output);

#endif
