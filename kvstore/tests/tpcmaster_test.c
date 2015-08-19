#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include "kvserver.h"
#include "socket_server.h"
#include "tpcmaster.h"
#include "tester.h"
#include <sys/socket.h>
#include <netdb.h>

#define KVSERVER_DIRNAME "tpcslave-test"
#define SLAVE_PORT 1234

tpcmaster_t testmaster;
kvserver_t testslaves[10];
char buf[20];
kvmessage_t reqmsg, respmsg;
int done = 0; /* Used for synchronizing some of the concurrency tests. */

typedef enum {
  GET_SIMPLE,
  GET_REPLICA,
  GET_FAIL,
  PUT_SIMPLE,
  PUT_ABORT,
  PUT_FAIL,
  DEL_SIMPLE,
  DEL_ABORT,
  DEL_FAIL,
  INFO_SIMPLE,
  INFO_FAIL,
} test_t;

test_t current_test;

pthread_mutex_t tpcmaster_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t tpcmaster_cond = PTHREAD_COND_INITIALIZER;

server_t socket_server;

struct arg_struct {
    int arg1;
    int arg2;
};

void setup_slaves();
void cleanup_slaves();
int setup_listen_socket(int);
int tpcmaster_run_test(void);

int tpcmaster_test_init(void) {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  srand(tv.tv_usec);
  memset(&reqmsg, 0, sizeof(kvmessage_t));
  memset(&respmsg, 0, sizeof(kvmessage_t));
  tpcmaster_init(&testmaster, 4, 2, 4, 4);
  return 1;
}

int tpcmaster_register_single(void) {
  reqmsg.type = REGISTER;
  reqmsg.key = "123.45.67.89";
  reqmsg.value = "987";
  tpcmaster_register(&testmaster, &reqmsg, &respmsg);
  ASSERT_STRING_EQUAL(respmsg.message, MSG_SUCCESS);
  tpcslave_t *slave = testmaster.slaves_head;
  ASSERT_STRING_EQUAL(slave->host, "123.45.67.89");
  ASSERT_EQUAL(slave->port, 987);
  ASSERT_EQUAL(slave->id, 5331380280059431172);
  return 1;
}


int tpcmaster_register_fail(void) {
  reqmsg.type = REGISTER;
  reqmsg.key = "123.45.67.89";
  reqmsg.value = "1234";
  tpcmaster_register(&testmaster, &reqmsg, &respmsg);
  reqmsg.value = "2345";
  tpcmaster_register(&testmaster, &reqmsg, &respmsg);
  reqmsg.value = "3456";
  tpcmaster_register(&testmaster, &reqmsg, &respmsg);
  reqmsg.value = "4567";
  tpcmaster_register(&testmaster, &reqmsg, &respmsg);
  reqmsg.value = "5678";
  tpcmaster_register(&testmaster, &reqmsg, &respmsg);
  ASSERT_STRING_EQUAL(respmsg.message, ERRMSG_GENERIC_ERROR);
  return 1;
}

int tpcmaster_get_slave_for_key(void) {
  setup_slaves();
  // hash is -1848860354761560747
  tpcslave_t *result = tpcmaster_get_primary(&testmaster, "winteriscoming");
  ASSERT_PTR_NOT_NULL(result);
  ASSERT_EQUAL(result->id, 2561935789451811312);
  // hash is 4971736480875674229
  result = tpcmaster_get_primary(&testmaster, "inagalaxyfarfaraway");
  ASSERT_PTR_NOT_NULL(result);
  ASSERT_EQUAL(result->id, 5397345852215556464);
  // hash is 610258952914415519
  result = tpcmaster_get_primary(&testmaster, "iamyourfather");
  ASSERT_PTR_NOT_NULL(result);
  ASSERT_EQUAL(result->id, 2561935789451811312);
  // hash is -2806306382981094914
  result = tpcmaster_get_primary(&testmaster, "thisisourtownscrub");
  ASSERT_PTR_NOT_NULL(result);
  ASSERT_EQUAL(result->id, -2561935789451811312);
  // hash is -4289169641250935895
  result = tpcmaster_get_primary(&testmaster, "noooooooo");
  ASSERT_PTR_NOT_NULL(result);
  ASSERT_EQUAL(result->id, -2561935789451811312);
  cleanup_slaves();
  return 1;
}

int tpcmaster_get_successor_for_slave(void) {
  setup_slaves();
  // hash is -1848860354761560747
  tpcslave_t *result = tpcmaster_get_successor(&testmaster,
      testmaster.slaves_head);
  ASSERT_PTR_NOT_NULL(result);
  ASSERT_EQUAL(result->id, -2561935789451811312);
  // hash is 4971736480875674229
  result = tpcmaster_get_successor(&testmaster, result);
  ASSERT_PTR_NOT_NULL(result);
  ASSERT_EQUAL(result->id, 2561935789451811312);
  // hash is 610258952914415519
  result = tpcmaster_get_successor(&testmaster, result);
  ASSERT_PTR_NOT_NULL(result);
  ASSERT_EQUAL(result->id, 5397345852215556464);
  // hash is -2806306382981094914
  result = tpcmaster_get_successor(&testmaster, result);
  ASSERT_PTR_NOT_NULL(result);
  ASSERT_EQUAL(result->id, -5397345852215556464);
  // hash is -4289169641250935895
  cleanup_slaves();
  return 1;
}

int tpcmaster_get_cached(void) {
  int ret;
  pthread_rwlock_t *cachelock = kvcache_getlock(&testmaster.cache, "KEY");
  pthread_rwlock_wrlock(cachelock);
  ret = kvcache_put(&testmaster.cache, "KEY", "VAL");
  pthread_rwlock_unlock(cachelock);
  ASSERT_EQUAL(ret, 0);
  reqmsg.key = "KEY";
  tpcmaster_handle_get(&testmaster, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, GETRESP);
  ASSERT_STRING_EQUAL(respmsg.key, "KEY");
  ASSERT_STRING_EQUAL(respmsg.value, "VAL");
  return 1;
}

void tpcmaster_dummy_handle(tpcmaster_t *master, int sockfd, callback_t callback) {
  kvmessage_t *req, resp;
  req = kvmessage_parse(sockfd);
  memset(&resp, 0, sizeof(kvmessage_t));
  switch (current_test) {
    case GET_SIMPLE:
      resp.type = GETRESP;
      resp.key = "KEY";
      resp.value = "VAL";
      break;
    case PUT_SIMPLE: case DEL_SIMPLE:
      if (req->type == PUTREQ || req->type == DELREQ)
        resp.type = VOTE_COMMIT;
      else
        resp.type = ACK;
      break;
    default:
      return;
  }
  kvmessage_send(&resp, sockfd);
  free(req);
}

void *tpcmaster_thread(void *aux) {
  reqmsg.key = "KEY";
  pthread_mutex_lock(&tpcmaster_lock);
  switch (current_test) {
    case GET_SIMPLE:
      reqmsg.type = GETREQ;
      tpcmaster_handle_get(&testmaster, &reqmsg, &respmsg);
      break;
    case PUT_SIMPLE:
      reqmsg.type = PUTREQ;
      reqmsg.value = "VAL";
      tpcmaster_handle_tpc(&testmaster, &reqmsg, &respmsg, NULL);
      break;
    case DEL_SIMPLE:
      reqmsg.type = DELREQ;
      tpcmaster_handle_tpc(&testmaster, &reqmsg, &respmsg, NULL);
      break;
    case INFO_SIMPLE:
      reqmsg.type = INFO;
      tpcmaster_info(&testmaster, &reqmsg, &respmsg);
      break;
    default:
      break;
  }
  done = 1;
  pthread_cond_signal(&tpcmaster_cond);
  pthread_mutex_unlock(&tpcmaster_lock);
  return 0;
}

int tpcmaster_get_simple(void) {
  current_test = GET_SIMPLE;
  tpcmaster_run_test();
  ASSERT_EQUAL(respmsg.type, GETRESP);
  ASSERT_STRING_EQUAL(respmsg.value, "VAL");
  return 1;
}

int tpcmaster_put_simple(void) {
  current_test = PUT_SIMPLE;
  tpcmaster_run_test();
  ASSERT_STRING_EQUAL(respmsg.message, MSG_SUCCESS);
  return 1;
}

int tpcmaster_del_simple(void) {
  current_test = DEL_SIMPLE;
  tpcmaster_run_test();
  ASSERT_STRING_EQUAL(respmsg.message, MSG_SUCCESS);
  return 1;
}

int tpcmaster_get_replica(void) {
  current_test = GET_REPLICA;
  tpcmaster_run_test();
  ASSERT_EQUAL(respmsg.type, GETRESP);
  ASSERT_STRING_EQUAL(respmsg.value, "VAL");
  return 1;
}

int tpcmaster_get_fail(void) {
  current_test = GET_FAIL;
  tpcmaster_run_test();
  ASSERT_STRING_EQUAL(respmsg.message, ERRMSG_NO_KEY);
  return 1;
}

int tpcmaster_put_abort(void) {
  current_test = PUT_ABORT;
  tpcmaster_run_test();
  ASSERT_STRING_EQUAL(respmsg.message, ERRMSG_GENERIC_ERROR);
  return 1;
}

int tpcmaster_put_fail(void) {
  current_test = PUT_FAIL;
  tpcmaster_run_test();
  ASSERT_STRING_EQUAL(respmsg.message, ERRMSG_GENERIC_ERROR);
  return 1;
}

int tpcmaster_info_check(void) {
  current_test = INFO_SIMPLE;
  tpcmaster_run_test();
  char buf[100], pbuf[10], expected[100];
  strcpy(buf, respmsg.message + strcspn(respmsg.message, "\n") + 1);
  sprintf(pbuf, "%d", SLAVE_PORT);
  strcpy(expected, "Slaves:\n{localhost, ");
  strcat(expected, pbuf);
  strcat(expected, "}\n{localhost, ");
  strcat(expected, pbuf);
  strcat(expected, "}\n{localhost, ");
  strcat(expected, pbuf);
  strcat(expected, "}\n{localhost, ");
  strcat(expected, pbuf);
  strcat(expected, "}");
  ASSERT_STRING_EQUAL(buf, expected);
  return 1;
}


void tpcmaster_test_connect(void) {
  pthread_t thread;
  pthread_create(&thread, NULL, &tpcmaster_thread, NULL);
}

void *tpcmaster_runner(void *callback) {
  socket_server.master = 1;
  socket_server.max_threads = 2;
  memcpy(&socket_server.tpcmaster, &testmaster, sizeof(tpcmaster_t));
  socket_server.tpcmaster.handle = tpcmaster_dummy_handle;
  server_run("test", SLAVE_PORT, &socket_server, (callback_t) callback);
  return NULL;
}

int tpcmaster_run_test(void) {
  setup_slaves();
  pthread_t runner;
  pthread_create(&runner, NULL, &tpcmaster_runner, tpcmaster_test_connect);
  pthread_mutex_lock(&tpcmaster_lock);
  while (done == 0)
    pthread_cond_wait(&tpcmaster_cond, &tpcmaster_lock);
  pthread_mutex_unlock(&tpcmaster_lock);
  server_stop(&socket_server);
  cleanup_slaves();
  return 1;
}

void setup_slaves() {
  int port = SLAVE_PORT;
  tpcslave_t *first = malloc(sizeof(tpcslave_t));
  first->host = "localhost";
  first->port = port;
  first->id = -5397345852215556464;
  tpcslave_t *second = malloc(sizeof(tpcslave_t));
  second->host = "localhost";
  second->port = port;
  second->id = -2561935789451811312;
  tpcslave_t *third = malloc(sizeof(tpcslave_t));
  third->host = "localhost";
  third->port = port;
  third->id = 2561935789451811312;
  tpcslave_t *fourth = malloc(sizeof(tpcslave_t));
  fourth->host = "localhost";
  fourth->port = port;
  fourth->id = 5397345852215556464;
  first->next = second;
  first->prev = fourth;
  second->next = third;
  second->prev = first;
  third->next = fourth;
  third->prev = second;
  fourth->next = first;
  fourth->prev = third;
  testmaster.slaves_head = first;
  testmaster.slave_count = 4;
}

void cleanup_slaves() {
  int i = 0;
  tpcslave_t *curr = testmaster.slaves_head;
  tpcslave_t *next = curr->next;
  while (i < 4) {
    free(curr);
    curr = next;
    next = curr->next;
    i++;
  }
}

test_info_t tpcmaster_tests[] = {
  {"Register a single slave", tpcmaster_register_single},
  {"Register one too many slaves", tpcmaster_register_fail},
  {"Identify first replica for multiple keys", tpcmaster_get_slave_for_key},
  {"Identify successor for multiple slaves", tpcmaster_get_successor_for_slave},
  {"Master GET value from master cache", tpcmaster_get_cached},
  {"Master GET value from main slave", tpcmaster_get_simple},
  {"Master PUT value", tpcmaster_put_simple},
  {"Master DEL value", tpcmaster_del_simple},
  {"Get information, all slaves", tpcmaster_info_check},
  NULL_TEST_INFO
};

suite_info_t tpcmaster_suite = {"TPCMaster Tests", tpcmaster_test_init, NULL,
  tpcmaster_tests};
