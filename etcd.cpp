#include "etcd.h"
#include <unistd.h>
#include "Logger.h"
#include <iostream>  

std::vector<std::string> ETCD::_client;
int ETCD::_index = 0;
Mutex ETCD::_instance_mutex;
Mutex ETCD::_op_mutex;
Mutex ETCD::_choose_mutex;
ETCD* ETCD::_cluster = NULL;
pthread_t ETCD::_update_thread_pid;
std::string ETCD::_username;
std::string ETCD::_password;

void* ETCD::update(void *param) {
  while(true) {
    std::vector<std::string> clients;
    int ret = ETCD::_get_all_members(clients); 
    if(ret == 0 && clients.size() > 0) {
      reset(clients);
    }
    sleep(10);
  }
}

int ETCD::_get_all_members(std::vector<std::string> &clients) {
  std::map<std::string, std::string> value;
  AutoLock<Mutex> lock(&_op_mutex);
  CURL *easy_handle = make_curl_handle();
  CurlHandleWrapper wrapper(easy_handle);
  char URI[512];
  std::string buff;
  char error_buff[512] = {'\0'};
  std::string addr = choose();
  snprintf(URI, sizeof(URI), "%s/%s", addr.c_str(), "v2/members");
  curl_easy_setopt(easy_handle, CURLOPT_URL, URI);
  curl_easy_setopt(easy_handle, CURLOPT_ERRORBUFFER, error_buff);
  curl_easy_setopt(easy_handle, CURLOPT_WRITEDATA, &buff);
  curl_easy_setopt(easy_handle, CURLOPT_WRITEFUNCTION, &collect_data);
  if(_username != "") {
    curl_easy_setopt(easy_handle, CURLOPT_USERNAME, _username.c_str());
    curl_easy_setopt(easy_handle, CURLOPT_PASSWORD, _password.c_str());
  }
  CURLcode ret = curl_easy_perform(easy_handle);
  if(ret != CURLE_OK) {
    LOG_ERROR(debug_log, "ETCD::_get_all_members error: %s", error_buff);
    return -1;
  } else {
    Json::Value json_value;
    Json::Reader reader;
    if(!reader.parse(buff, json_value)) {
      LOG_ERROR(debug_log, "ETCD::_get_all_members error: %s", buff.c_str());
      return -1;
    } else {
      for(int i = 0; i < json_value["members"].size(); ++i) {
        for(int j = 0; j < json_value["members"][i]["clientURLs"].size(); ++j) {
          clients.push_back(json_value["members"][i]["clientURLs"][j].asString());
        }
      }
    }
  }
  return 0;
}

int ETCD::init(const std::string &seed, const std::string &username, const std::string &password){
  _client.push_back(seed);
  _username = username;
  _password = password;
  
  CURLcode ret =  curl_global_init(CURL_GLOBAL_SSL);  
  if(ret != CURLE_OK) {
    return -1;
  }
 
  std::vector<std::string> clients;
  clients.push_back(seed);
  int retv = ETCD::_get_all_members(clients);
  if(retv == 0) {
    ETCD::reset(clients); 
    return 0;
  }
  return -1;
}

int ETCD::set(const std::string &key, const std::string &value) {
  int retry = 0;
  int max_retry = 10;
  while(retry < max_retry) {
    std::string client = choose();
    try {
      int ret = _set(client, "v2/keys/" + key, value);
      if(ret == 0) {
        return 0;
      }
    } catch (ETCD_Exception &e) {
      std::cerr << "ETCD:set " << e.what() << std::endl;
      LOG_ERROR(debug_log, "ETCD:set %s", e.what());   
    }
    retry++;
  }
  return -1;
}

int ETCD::_set(const std::string &addr, const std::string &key, const std::string &value) {
  AutoLock<Mutex> lock(&_op_mutex);
  CURL *easy_handle = make_curl_handle();
  CurlHandleWrapper wrapper(easy_handle);
  std::string buff; 
  char URI[512];
  char error_buff[512] = {'\0'};
  std::string etcd_value = "value=" + value;
  snprintf(URI, sizeof(URI), "%s/%s", addr.c_str(), key.c_str());
  curl_easy_setopt(easy_handle, CURLOPT_URL, URI);
  curl_easy_setopt(easy_handle, CURLOPT_POST, 1);  
  curl_easy_setopt(easy_handle, CURLOPT_ERRORBUFFER, error_buff);
  curl_easy_setopt(easy_handle, CURLOPT_POSTFIELDS, etcd_value.c_str());  
  
  curl_easy_setopt(easy_handle, CURLOPT_WRITEDATA, &buff);
  curl_easy_setopt(easy_handle, CURLOPT_WRITEFUNCTION, &collect_data);
  
  if(_username != "") {
    curl_easy_setopt(easy_handle, CURLOPT_USERNAME, _username.c_str());
    curl_easy_setopt(easy_handle, CURLOPT_PASSWORD, _password.c_str());
  }

  curl_easy_setopt(easy_handle, CURLOPT_CUSTOMREQUEST, "PUT");
  CURLcode ret = curl_easy_perform(easy_handle);
  
  if(ret != CURLE_OK) {
    throw ETCD_Exception("ETCD:_set " + std::string(error_buff));
  }

  Json::Value root;
  Json::Reader reader;
  reader.parse(buff, root); 
  if(root.isMember("errorCode")) {
    throw ETCD_Exception("ETCD:_set " + buff);
  }
  return 0;
}

int ETCD::get(const std::string &key, std::string &value) {
  int retry = 0;
  int max_retry = 10;
  while(retry < max_retry) {
    std::string client = choose();
    std::map<std::string, std::string> v_map;
    try {
      int ret = _get(client, "v2/keys/" + key, v_map);
      if(ret == 0) {
        value = v_map["value"];
        return 0;
      }
    } catch (ETCD_Exception &e) {
      LOG_ERROR(debug_log, "ETCD:get %s", e.what());
      std::cerr << "ETCD:get " << e.what() << std::endl;
    }
    retry++;
  }
  return -1;
}

int ETCD::get(const std::string &key, std::map<std::string, std::string> &value) {
  int retry = 0;
  int max_retry = 10;
  while(retry < max_retry) {
    std::string client = choose();
    try {
      int ret = _get(client, "v2/keys/" + key, value);
      if(ret == 0) {
        return 0;
      } 
    } catch (ETCD_Exception &e) {
      LOG_ERROR(debug_log, "ETCD:get %s", e.what());   
      std::cerr << "ETCD:get " << e.what() << std::endl;
    }
    retry++;
  }
  return -1;
}

int ETCD::_get(const std::string &addr, const std::string &key, std::map<std::string, std::string> &value) {
  char URI[512];
  std::string buff;
  char error_buff[512] = {'\0'};
  AutoLock<Mutex> lock(&_op_mutex);
  CURL *easy_handle = make_curl_handle();
  CurlHandleWrapper wrapper(easy_handle);
  snprintf(URI, sizeof(URI), "%s/%s", addr.c_str(), key.c_str());
  curl_easy_setopt(easy_handle, CURLOPT_URL, URI);
  curl_easy_setopt(easy_handle, CURLOPT_ERRORBUFFER, error_buff);
  curl_easy_setopt(easy_handle, CURLOPT_WRITEDATA, &buff);
  curl_easy_setopt(easy_handle, CURLOPT_WRITEFUNCTION, &collect_data);
  
  if(_username != "") {
    curl_easy_setopt(easy_handle, CURLOPT_USERNAME, _username.c_str());
    curl_easy_setopt(easy_handle, CURLOPT_PASSWORD, _password.c_str());
  }

  CURLcode ret = curl_easy_perform(easy_handle);
  
  if(ret != CURLE_OK) {
    throw ETCD_Exception("ETCD:_get " + std::string(error_buff));
  }

  Json::Value root;
  Json::Reader reader;
  reader.parse(buff, root); 
  if(root.isMember("errorCode")) {
    throw ETCD_Exception("ETCD:_get " + buff);
  }
  return process_data(buff, value);
}

int ETCD::compare_and_set(const std::string &key, const std::string &prev_modify_index, const std::string &new_value) {
  std::string client = choose();
  try {
    int ret = _compare_and_set(client, "v2/keys/" + key, prev_modify_index, new_value);
    return ret;
  } catch (ETCD_Exception &e) {
    std::cerr << "ETCD:compare_and_set " << e.what() << std::endl;
    LOG_ERROR(debug_log, "ETCD:compare_and_set %s", e.what());   
    return -1;
  }
  return 0;
}

int ETCD::_compare_and_set(const std::string &addr, const std::string &key, const std::string &prev_modify_index, const std::string &new_value) {
  AutoLock<Mutex> lock(&_op_mutex);
  CURL *easy_handle = make_curl_handle();
  CurlHandleWrapper wrapper(easy_handle);
  std::string buff;
  char URI[512];
  char error_buff[512] = {'\0'};
  std::string etcd_value = "prevIndex=" + prev_modify_index;
  etcd_value += "&value=" + new_value;
  snprintf(URI, sizeof(URI), "%s/%s", addr.c_str(), key.c_str());
  curl_easy_setopt(easy_handle, CURLOPT_URL, URI);
  curl_easy_setopt(easy_handle, CURLOPT_POST, 1);  
  curl_easy_setopt(easy_handle, CURLOPT_ERRORBUFFER, error_buff);
  curl_easy_setopt(easy_handle, CURLOPT_POSTFIELDS, etcd_value.c_str());  

  if(_username != "") {
    curl_easy_setopt(easy_handle, CURLOPT_USERNAME, _username.c_str());
    curl_easy_setopt(easy_handle, CURLOPT_PASSWORD, _password.c_str());
  }

  curl_easy_setopt(easy_handle, CURLOPT_WRITEDATA, &buff);
  curl_easy_setopt(easy_handle, CURLOPT_WRITEFUNCTION, &collect_data);
  curl_easy_setopt(easy_handle, CURLOPT_CUSTOMREQUEST, "PUT");
  CURLcode ret = curl_easy_perform(easy_handle);
  if(ret != CURLE_OK) {
    throw ETCD_Exception("ETCD::_compare_and_set " + std::string(error_buff));
  }

  Json::Value root;
  Json::Reader reader;
  reader.parse(buff, root); 
  
  if(root.isMember("errorCode")) {
    throw ETCD_Exception("ETCD::_compare_and_set " + buff);
  }
  return 0;
}

int process_data(const std::string &input, std::map<std::string, std::string> &output) {
  Json::Value node, root;
  Json::Reader reader;
  Json::FastWriter writer;
  //Json::StyledWriter writer;
  std::string json = input;
  
  if(!reader.parse(json, root)){
    return -1;
  }

  std::string nodeString = writer.write(root["node"]);
 
  if(!reader.parse(nodeString, node)) {
    return -1;
  }
  std::vector<std::string> members = node.getMemberNames();
  for(int i = 0; i < members.size(); ++i) {
    //writer写出的字符串带有转义符，这个传给reader解析后，结果不符合预期，暂时用asString代替
    //output[members[i]] = writer.write(node[members[i]]);
    output[members[i]] = node[members[i]].asString();
  }
  return 0;
}

size_t collect_data(char *buffer, size_t size, size_t nmemb, void *user_p){
  std::string *buff = (std::string*)user_p;
  /*  
  for(int i = 0; i < nmemb; ++i) {
    *buff = *buff + buffer[i];
  }
  */
  *buff = *buff + std::string(buffer, nmemb);
  return size * nmemb;
}
