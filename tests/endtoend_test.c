#include <unistd.h>
#include <sys/socket.h>
#include <pthread.h>
#include "tester.h"
#include "socket_server.h"

#define ENDTOEND_HOSTNAME "localhost"
#define ENDTOEND_PORT 8162
#define ENDTOEND_SERVER_NAME "endtoend_server"
#define NUM_THREADS 20

server_t socket_server;
kvserver_t *kvserver;
pthread_mutex_t endtoend_lock;
pthread_cond_t endtoend_cond;
int synch;

int endtoend_test_init(void) {
  pthread_mutex_init(&endtoend_lock, NULL);
  pthread_cond_init(&endtoend_cond, NULL);

  socket_server.master = 0;
  socket_server.max_threads = 2;
  kvserver = &socket_server.kvserver;
  return kvserver_init(kvserver, ENDTOEND_SERVER_NAME, 2, 2, 2,
      ENDTOEND_HOSTNAME, ENDTOEND_PORT, 0);
}

int endtoend_test_clean(void) {
  return kvserver_clean(kvserver);
}

kvmessage_t *endtoend_send_and_receive(kvmessage_t *reqmsg) {
  kvmessage_t *respmsg;
  int sockfd;
  sockfd = connect_to(ENDTOEND_HOSTNAME, ENDTOEND_PORT, 3);
  kvmessage_send(reqmsg, sockfd);
  respmsg = kvmessage_parse(sockfd);
  shutdown(sockfd, SHUT_RDWR);
  close(sockfd);
  return respmsg;
}

void *endtoend_test_client_thread(void *aux) {
  kvmessage_t reqmsg, *respmsg;
  int pass = 1;

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = PUTREQ;
  reqmsg.key = "key1";
  reqmsg.value = "value1";
  respmsg = endtoend_send_and_receive(&reqmsg);
  if (respmsg->type != RESP)
    pass = 0;
  kvmessage_free(respmsg);

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = GETREQ;
  reqmsg.key = "key1";
  respmsg = endtoend_send_and_receive(&reqmsg);
  if (respmsg->type != GETRESP || strcmp(respmsg->key, "key1") != 0
      || strcmp(respmsg->value, "value1") != 0)
    pass = 0;
  kvmessage_free(respmsg);

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = PUTREQ;
  reqmsg.key = "key1";
  reqmsg.value = "newvalue";
  respmsg = endtoend_send_and_receive(&reqmsg);
  if (respmsg->type != RESP)
    pass = 0;
  kvmessage_free(respmsg);

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = GETREQ;
  reqmsg.key = "key1";
  respmsg = endtoend_send_and_receive(&reqmsg);
  if (respmsg->type != GETRESP || strcmp(respmsg->key, "key1") != 0
      || strcmp(respmsg->value, "newvalue") != 0)
    pass = 0;
  kvmessage_free(respmsg);

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = DELREQ;
  reqmsg.key = "key1";
  respmsg = endtoend_send_and_receive(&reqmsg);
  if (respmsg->type != RESP)
    pass = 0;
  kvmessage_free(respmsg);

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = GETREQ;
  reqmsg.key = "key1";
  respmsg = endtoend_send_and_receive(&reqmsg);
  if (respmsg->type != RESP ||
      strcmp(respmsg->message, ERRMSG_NO_KEY) != 0)
    pass = 0;
  kvmessage_free(respmsg);

  pthread_mutex_lock(&endtoend_lock);
  synch = pass;
  pthread_cond_signal(&endtoend_cond);
  pthread_mutex_unlock(&endtoend_lock);
  return 0;
}

void endtoend_test_connect() {
  pthread_t thread;
  pthread_create(&thread, NULL, &endtoend_test_client_thread, NULL);
}

void *endtoend_server_runner(void *callback){
  server_run(ENDTOEND_HOSTNAME, ENDTOEND_PORT, &socket_server,
      (callback_t) callback);
  return NULL;
}

int endtoend_test(void) {
  int pass;
  pthread_t server_thread;
  pthread_create(&server_thread, NULL, &endtoend_server_runner,
      endtoend_test_connect);

  pthread_mutex_lock(&endtoend_lock);
  pthread_cond_wait(&endtoend_cond, &endtoend_lock);
  pass = (synch == 1);
  pthread_mutex_unlock(&endtoend_lock);

  server_stop(&socket_server);
  ASSERT_TRUE(pass);
  return 1;
}

void *e2e_weird_client_thread(void *aux) {
  kvmessage_t reqmsg, *respmsg;
  int pass = 1;

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = PUTREQ;
  reqmsg.key = "k\n\x0d\x04";
  reqmsg.value = "";
  respmsg = endtoend_send_and_receive(&reqmsg);
  if (respmsg->type != RESP)
    pass = 0;

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = GETREQ;
  reqmsg.key = "k\n\x0d\x04";
  respmsg = endtoend_send_and_receive(&reqmsg);
  if (respmsg->type != GETRESP || strcmp(respmsg->key, "k\n\x0d\x04") != 0
      || strcmp(respmsg->value, "") != 0)
    pass = 0;
  kvmessage_free(respmsg);

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = PUTREQ;
  reqmsg.key = "k\n\n\0\n";
  reqmsg.value = "v\x0d\x04\n\0nope";
  respmsg = endtoend_send_and_receive(&reqmsg);
  if (respmsg->type != RESP)
    pass = 0;
  kvmessage_free(respmsg);

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = GETREQ;
  reqmsg.key = "k\n\n\0\n";
  respmsg = endtoend_send_and_receive(&reqmsg);
  if (respmsg->type != GETRESP || strcmp(respmsg->key, "k\n\n") != 0
      || strcmp(respmsg->value, "v\x0d\x04\n") != 0)
    pass = 0;
  kvmessage_free(respmsg);

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = DELREQ;
  reqmsg.key = "k\n\n";
  respmsg = endtoend_send_and_receive(&reqmsg);
  if (respmsg->type != RESP)
    pass = 0;
  kvmessage_free(respmsg);

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = PUTREQ;
  reqmsg.key = "k\x07\x06\0derpaderpq";
  reqmsg.value = "Shadow Word: Pain on a Deathlord";
  respmsg = endtoend_send_and_receive(&reqmsg);
  if (respmsg->type != RESP)
    pass = 0;
  kvmessage_free(respmsg);

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = DELREQ;
  reqmsg.key = "k\x07\x06\0doesn't matter";
  respmsg = endtoend_send_and_receive(&reqmsg);
  if (respmsg->type != RESP)
    pass = 0;
  kvmessage_free(respmsg);

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = DELREQ;
  reqmsg.key = "k\x07\x06\0still doesn't matter";
  respmsg = endtoend_send_and_receive(&reqmsg);
  if (respmsg->type != RESP
     || strcmp(respmsg->message, ERRMSG_NO_KEY) != 0)
    pass = 0;
  kvmessage_free(respmsg);

  pthread_mutex_lock(&endtoend_lock);
  synch = pass;
  pthread_cond_signal(&endtoend_cond);
  pthread_mutex_unlock(&endtoend_lock);
  return 0;
}

void e2e_weird_test_connect() {
  pthread_t thread;
  pthread_create(&thread, NULL, &e2e_weird_client_thread, NULL);
}

int e2e_weird_test(void) {
  int pass;
  pthread_t server_thread;
  pthread_create(&server_thread, NULL, &endtoend_server_runner,
      e2e_weird_test_connect);

  pthread_mutex_lock(&endtoend_lock);
  pthread_cond_wait(&endtoend_cond, &endtoend_lock);
  pass = (synch == 1);
  pthread_mutex_unlock(&endtoend_lock);

  server_stop(&socket_server);
  ASSERT_TRUE(pass);
  return 1;
}

void *e2e_lots_of_clients_thread(void *aux) {
  kvmessage_t reqmsg, *respmsg = NULL;
  int pass = 1;

  //Initialize with PUT requests, should be idempotent 
  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = PUTREQ;
  reqmsg.key = "k1";
  reqmsg.value = "v1";
  while(!respmsg) {
    respmsg = endtoend_send_and_receive(&reqmsg);
  }
  if (respmsg->type != RESP)
    pass = 0;
  kvmessage_free(respmsg);
  respmsg = NULL;

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = PUTREQ;
  reqmsg.key = "k2";
  reqmsg.value = "v2";
  while(!respmsg) {
    respmsg = endtoend_send_and_receive(&reqmsg);
  }
  if (respmsg->type != RESP)
    pass = 0;
  kvmessage_free(respmsg);
  respmsg = NULL;

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = PUTREQ;
  reqmsg.key = "k3";
  reqmsg.value = "v3";
  while(!respmsg) {
    respmsg = endtoend_send_and_receive(&reqmsg);
  }
  if (respmsg->type != RESP)
    pass = 0;
  kvmessage_free(respmsg);
  respmsg = NULL;

  /** Perform a mix of get requests and redundant put requests
   *  for consistency between threads
   */
  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = GETREQ;
  reqmsg.key = "k2";
  while(!respmsg) {
    respmsg = endtoend_send_and_receive(&reqmsg);
  }
  if (respmsg->type != GETRESP || strcmp(respmsg->key, "k2") != 0
      || strcmp(respmsg->value, "v2") != 0)
    pass = 0;
  kvmessage_free(respmsg);
  respmsg = NULL;

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = PUTREQ;
  reqmsg.key = "k3";
  reqmsg.value = "v3";
  while(!respmsg) {
    respmsg = endtoend_send_and_receive(&reqmsg);
  }
  if (respmsg->type != RESP)
    pass = 0;
  kvmessage_free(respmsg);
  respmsg = NULL;

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = GETREQ;
  reqmsg.key = "k1";
  while(!respmsg) {
    respmsg = endtoend_send_and_receive(&reqmsg);
  }
  if (respmsg->type != GETRESP || strcmp(respmsg->key, "k1") != 0
      || strcmp(respmsg->value, "v1") != 0)
    pass = 0;
  kvmessage_free(respmsg);
  respmsg = NULL;

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = GETREQ;
  reqmsg.key = "k2";
  while(!respmsg) {
    respmsg = endtoend_send_and_receive(&reqmsg);
  }
  if (respmsg->type != GETRESP || strcmp(respmsg->key, "k2") != 0
      || strcmp(respmsg->value, "v2") != 0)
    pass = 0;
  kvmessage_free(respmsg);
  respmsg = NULL;

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = GETREQ;
  reqmsg.key = "k3";
  while(!respmsg) {
    respmsg = endtoend_send_and_receive(&reqmsg);
  }
  if (respmsg->type != GETRESP || strcmp(respmsg->key, "k3") != 0
      || strcmp(respmsg->value, "v3") != 0)
    pass = 0;
  kvmessage_free(respmsg);
  respmsg = NULL;

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = PUTREQ;
  reqmsg.key = "k3";
  reqmsg.value = "v3";
  while(!respmsg) {
    respmsg = endtoend_send_and_receive(&reqmsg);
  }
  if (respmsg->type != RESP)
    pass = 0;
  kvmessage_free(respmsg);
  respmsg = NULL;

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = GETREQ;
  reqmsg.key = "k2";
  while(!respmsg) {
    respmsg = endtoend_send_and_receive(&reqmsg);
  }
  if (respmsg->type != GETRESP || strcmp(respmsg->key, "k2") != 0
      || strcmp(respmsg->value, "v2") != 0)
    pass = 0;
  kvmessage_free(respmsg);
  respmsg = NULL;

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = PUTREQ;
  reqmsg.key = "k3";
  reqmsg.value = "v3";
  while(!respmsg) {
    respmsg = endtoend_send_and_receive(&reqmsg);
  }
  if (respmsg->type != RESP)
    pass = 0;
  kvmessage_free(respmsg);
  respmsg = NULL;

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = GETREQ;
  reqmsg.key = "k2";
  while(!respmsg) {
    respmsg = endtoend_send_and_receive(&reqmsg);
  }
  if (respmsg->type != GETRESP || strcmp(respmsg->key, "k2") != 0
      || strcmp(respmsg->value, "v2") != 0)
    pass = 0;
  kvmessage_free(respmsg);
  respmsg = NULL;

  pthread_mutex_lock(&endtoend_lock);
  synch += pass;
  pthread_mutex_unlock(&endtoend_lock);
  return 0;
}

void e2e_lots_of_clients_creator_thread() {
  int i;
  void* ret;
  pthread_t threads[NUM_THREADS];
  for(i = 0; i < NUM_THREADS; i++) {
    pthread_create(&threads[i], NULL, &e2e_lots_of_clients_thread, NULL);
  }
  for(i = 0; i < NUM_THREADS; i++) {
    pthread_join(threads[i], &ret);
  }
  pthread_mutex_lock(&endtoend_lock);
  pthread_cond_signal(&endtoend_cond);
  pthread_mutex_unlock(&endtoend_lock);
}

void e2e_lots_of_clients_connect() {
  pthread_t clients_thread;
  pthread_create(&clients_thread, NULL, &e2e_lots_of_clients_creator_thread, NULL);
}

int e2e_lots_of_clients(void) {
  int pass;
  synch = 0;
  pthread_t server_thread;
  pthread_create(&server_thread, NULL, &endtoend_server_runner,
      e2e_lots_of_clients_connect);

  pthread_mutex_lock(&endtoend_lock);
  pthread_cond_wait(&endtoend_cond, &endtoend_lock);
  pass = (synch == NUM_THREADS);
  pthread_mutex_unlock(&endtoend_lock);

  server_stop(&socket_server);
  ASSERT_TRUE(pass);
  return 1;
}

test_info_t endtoend_tests[] = {
  {"End to end test placing keys, deleting them, getting them", endtoend_test},
  {"End to end test with weird k/v get/put/del", e2e_weird_test},
  {"End to end test with more clients than threads", e2e_lots_of_clients},
  NULL_TEST_INFO
};

suite_info_t endtoend_suite = {"EndToEnd Tests", endtoend_test_init,
  endtoend_test_clean, endtoend_tests};
