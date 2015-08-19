#include <unistd.h>
#include <sys/socket.h>
#include <pthread.h>
#include "tester.h"
#include "socket_server.h"

#define ENDTOEND_HOSTNAME "localhost"
#define ENDTOEND_PORT 8162
#define ENDTOEND_SERVER_NAME "endtoend_server"

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

test_info_t endtoend_tests[] = {
  {"End to end test placing keys, deleting them, getting them", endtoend_test},
  NULL_TEST_INFO
};

suite_info_t endtoend_suite = {"EndToEnd Tests", endtoend_test_init,
  endtoend_test_clean, endtoend_tests};
