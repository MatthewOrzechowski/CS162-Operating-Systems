#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdbool.h>
#include <dirent.h>
#include <stdio.h>
#include <pthread.h>
#include "kvmessage.h"
#include "kvserver.h"
#include "socket_server.h"
#include "tester.h"

#define KVSERVER_TPC_HOSTNAME "localhost"
#define KVSERVER_TPC_PORT 8162
#define KVSERVER_TPC_DIRNAME "kvserver-test"

pthread_mutex_t kvserver_tpc_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t kvserver_tpc_cond = PTHREAD_COND_INITIALIZER;
int completed, synch;
server_t socket_server;

kvserver_t testserver;
kvmessage_t reqmsg, respmsg;

/* Deletes all current entries in the store and removes the store directory. */
int kvserver_tpc_test_clean(void) {
  return kvserver_clean(&testserver);
}

int kvserver_tpc_test_init(void) {
  memset(&reqmsg, 0, sizeof(kvmessage_t));
  memset(&respmsg, 0, sizeof(kvmessage_t));
  kvserver_init(&testserver, KVSERVER_TPC_DIRNAME, 4, 4, 1,
      KVSERVER_TPC_HOSTNAME, KVSERVER_TPC_PORT, true);
  return 0;
}

int kvserver_tpc_put_commit(void) {
  reqmsg.type = PUTREQ;
  reqmsg.key = "MYKEY";
  reqmsg.value = "MYVALUE";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, VOTE_COMMIT);

  /* Make sure that the key hasn't actually been added yet. */
  reqmsg.type = GETREQ;
  reqmsg.key = "MYKEY";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, ERRMSG_NO_KEY);

  reqmsg.type = COMMIT;
  reqmsg.key = NULL;
  reqmsg.value = NULL;
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, ACK);

  reqmsg.type = GETREQ;
  reqmsg.key = "MYKEY";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, GETRESP);
  ASSERT_STRING_EQUAL(respmsg.key, "MYKEY");
  ASSERT_STRING_EQUAL(respmsg.value, "MYVALUE");
  return 1;
}

int kvserver_tpc_put_multiple_commits(void) {
  reqmsg.type = PUTREQ;
  reqmsg.key = "MYKEY";
  reqmsg.value = "MYVALUE";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, VOTE_COMMIT);

  /* Make sure that the key hasn't actually been added yet. */
  reqmsg.type = GETREQ;
  reqmsg.key = "MYKEY";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, ERRMSG_NO_KEY);

  reqmsg.type = COMMIT;
  reqmsg.key = NULL;
  reqmsg.value = NULL;
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, ACK);

  memset(&respmsg, 0, sizeof(kvmessage_t));
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, ACK);

  reqmsg.type = GETREQ;
  reqmsg.key = "MYKEY";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, GETRESP);
  ASSERT_STRING_EQUAL(respmsg.key, "MYKEY");
  ASSERT_STRING_EQUAL(respmsg.value, "MYVALUE");

  reqmsg.type = COMMIT;
  reqmsg.key = NULL;
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, ACK);
  return 1;
}

int kvserver_tpc_put_abort(void) {
  reqmsg.type = PUTREQ;
  reqmsg.key = "MYKEY";
  reqmsg.value = "MYVALUE";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, VOTE_COMMIT);

  reqmsg.type = ABORT;
  reqmsg.key = NULL;
  reqmsg.value = NULL;
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, ACK);

  reqmsg.type = GETREQ;
  reqmsg.key = "MYKEY";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, ERRMSG_NO_KEY);
  return 1;
}

int kvserver_tpc_put_invalid(void) {
  char longkey[MAX_KEYLEN+2];
  int i;

  for (i = 0; i < MAX_KEYLEN+1; ++i)
    longkey[i] = 'a';
  longkey[i] = '\0';

  reqmsg.type = PUTREQ;
  reqmsg.key = longkey;
  reqmsg.value = "MYVALUE";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, VOTE_ABORT);
  ASSERT_STRING_EQUAL(respmsg.message, ERRMSG_KEY_LEN);

  /* Make sure that the key hasn't been added. */
  reqmsg.type = GETREQ;
  reqmsg.key = "MYKEY";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, ERRMSG_NO_KEY);

  /* Make sure the server is ready to handle another request. */
  reqmsg.type = PUTREQ;
  reqmsg.key = "MYKEY";
  reqmsg.value = "MYVALUE";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, VOTE_COMMIT);
  return 1;
}

int kvserver_tpc_del_commit(void) {
  reqmsg.type = PUTREQ;
  reqmsg.key = "MYKEY";
  reqmsg.value = "MYVALUE";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, VOTE_COMMIT);
  reqmsg.type = COMMIT;
  reqmsg.key = NULL;
  reqmsg.value = NULL;
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, ACK);

  reqmsg.type = DELREQ;
  reqmsg.key = "MYKEY";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, VOTE_COMMIT);

  /* Ensure that the key hasn't actually been deleted yet. */
  reqmsg.type = GETREQ;
  reqmsg.key = "MYKEY";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, GETRESP);
  ASSERT_STRING_EQUAL(respmsg.key, "MYKEY");
  ASSERT_STRING_EQUAL(respmsg.value, "MYVALUE");

  reqmsg.type = COMMIT;
  reqmsg.key = NULL;
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, ACK);

  reqmsg.type = GETREQ;
  reqmsg.key = "MYKEY";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, ERRMSG_NO_KEY);
  return 1;
}

int kvserver_tpc_del_invalid_keylen(void) {
  char longkey[MAX_KEYLEN + 2];
  int i;

  for (i = 0; i < MAX_KEYLEN + 1; ++i)
    longkey[i] = 'a';
  longkey[i] = '\0';

  reqmsg.type = DELREQ;
  reqmsg.key = longkey;
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, VOTE_ABORT);
  ASSERT_STRING_EQUAL(respmsg.message, ERRMSG_KEY_LEN);

  /* Make sure the server is ready to handle another request. */
  reqmsg.type = PUTREQ;
  reqmsg.key = "MYKEY";
  reqmsg.value = "MYVALUE";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, VOTE_COMMIT);
  return 1;
}

int kvserver_tpc_concurrent_del_put(void) {
  reqmsg.type = PUTREQ;
  reqmsg.key = "MYKEY1";
  reqmsg.value = "MYVALUE1";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, VOTE_COMMIT);
  reqmsg.type = COMMIT;
  reqmsg.key = NULL;
  reqmsg.value = NULL;
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, ACK);

  reqmsg.type = DELREQ;
  reqmsg.key = "MYKEY1";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, VOTE_COMMIT);

  reqmsg.type = PUTREQ;
  reqmsg.key = "MYKEY2";
  reqmsg.value = "MYVALUE2";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, ERRMSG_INVALID_REQUEST);
  return 1;
}

int kvserver_tpc_rebuild_single_put(void) {
  reqmsg.type = PUTREQ;
  reqmsg.key = "MYKEY1";
  reqmsg.value = "MYVALUE1";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);

  ASSERT_EQUAL(respmsg.type, VOTE_COMMIT);

  /* Simulate a crash + rebuild. */
  memset(&testserver, 0, sizeof(kvserver_t));
  kvserver_init(&testserver, KVSERVER_TPC_DIRNAME, 4, 4, 1,
      KVSERVER_TPC_HOSTNAME, KVSERVER_TPC_PORT, true);
  kvserver_rebuild_state(&testserver);

  reqmsg.key = "MYKEY2";
  reqmsg.value = "MYVALUE2";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);

  /* Check that server is in a TPC_READY state */
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, ERRMSG_INVALID_REQUEST);

  reqmsg.type = GETREQ;
  reqmsg.key = "MYKEY1";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, ERRMSG_NO_KEY);

  reqmsg.type = COMMIT;
  reqmsg.key = NULL;
  reqmsg.value = NULL;
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, ACK);

  reqmsg.type = GETREQ;
  reqmsg.key = "MYKEY1";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, GETRESP);
  ASSERT_STRING_EQUAL(respmsg.key, "MYKEY1");
  ASSERT_STRING_EQUAL(respmsg.value, "MYVALUE1");
  return 1;
}

int kvserver_tpc_rebuild_del(void) {
  reqmsg.type = PUTREQ;
  reqmsg.key = "MYKEY1";
  reqmsg.value = "MYVALUE1";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, VOTE_COMMIT);

  reqmsg.type = COMMIT;
  reqmsg.key = reqmsg.value = NULL;
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);

  reqmsg.type = DELREQ;
  reqmsg.key = "MYKEY1";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, VOTE_COMMIT);

  /* Simulate a crash + rebuild. */
  memset(&testserver, 0, sizeof(kvserver_t));
  kvserver_init(&testserver, KVSERVER_TPC_DIRNAME, 4, 4, 1,
      KVSERVER_TPC_HOSTNAME, KVSERVER_TPC_PORT, true);
  kvserver_rebuild_state(&testserver);

  /* Check that server is in a TPC_READY state */
  reqmsg.key = "MYKEY2";
  reqmsg.value = "MYVALUE2";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, ERRMSG_INVALID_REQUEST);

  /* Check that key has not yet been delete */
  reqmsg.type = GETREQ;
  reqmsg.key = "MYKEY1";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, GETRESP);
  ASSERT_STRING_EQUAL(respmsg.key, "MYKEY1");
  ASSERT_STRING_EQUAL(respmsg.value, "MYVALUE1");

  reqmsg.type = COMMIT;
  reqmsg.key = NULL;
  reqmsg.value = NULL;
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, ACK);

  reqmsg.type = GETREQ;
  reqmsg.key = "MYKEY1";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, ERRMSG_NO_KEY);
  return 1;
}

int kvserver_tpc_rebuild_abort(void) {
  reqmsg.type = PUTREQ;
  reqmsg.key = "MYKEY1";
  reqmsg.value = "MYVALUE1";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, VOTE_COMMIT);

  reqmsg.type = ABORT;
  reqmsg.key = reqmsg.value = NULL;
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, ACK);

  /* Simulate a crash + rebuild. */
  memset(&testserver, 0, sizeof(kvserver_t));
  kvserver_init(&testserver, KVSERVER_TPC_DIRNAME, 4, 4, 1,
      KVSERVER_TPC_HOSTNAME, KVSERVER_TPC_PORT, true);
  kvserver_rebuild_state(&testserver);

  /* Check that server is in a TPC_INIT state */
  reqmsg.type = PUTREQ;
  reqmsg.key = "MYKEY2";
  reqmsg.value = "MYVALUE2";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, VOTE_COMMIT);
  return 1;
}

int kvserver_tpc_rebuild_put_commit(void) {
  reqmsg.type = PUTREQ;
  reqmsg.key = "MYKEY1";
  reqmsg.value = "MYVALUE1";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, VOTE_COMMIT);

  reqmsg.type = COMMIT;
  reqmsg.key = reqmsg.value = NULL;
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, ACK);

  /* Simulate a crash. */
  memset(&testserver, 0, sizeof(kvserver_t));
  kvserver_init(&testserver, KVSERVER_TPC_DIRNAME, 4, 4, 1,
      KVSERVER_TPC_HOSTNAME, KVSERVER_TPC_PORT, true);

  /* Forcefully remove from store (simulate crashing after log but before store
   * complete) */
  kvstore_del(&testserver.store, "MYKEY1");

  kvserver_rebuild_state(&testserver);

  /* Check that server completed the transaction properly */
  reqmsg.type = GETREQ;
  reqmsg.key = "MYKEY1";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, GETRESP);
  ASSERT_STRING_EQUAL(respmsg.key, "MYKEY1");
  ASSERT_STRING_EQUAL(respmsg.value, "MYVALUE1");
  return 1;
}

int kvserver_tpc_rebuild_multiple_commits(void) {
  reqmsg.type = PUTREQ;
  reqmsg.key = "MYKEY1";
  reqmsg.value = "MYVALUE1";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, VOTE_COMMIT);

  reqmsg.type = COMMIT;
  reqmsg.key = reqmsg.value = NULL;
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, ACK);

  reqmsg.type = COMMIT;
  reqmsg.key = reqmsg.value = NULL;
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, ACK);

  reqmsg.type = COMMIT;
  reqmsg.key = reqmsg.value = NULL;
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, ACK);

  /* Simulate a crash. */
  memset(&testserver, 0, sizeof(kvserver_t));
  kvserver_init(&testserver, KVSERVER_TPC_DIRNAME, 4, 4, 1,
      KVSERVER_TPC_HOSTNAME, KVSERVER_TPC_PORT, true);

  /* Forcefully remove from store (simulate crashing after log but before store
   * complete) */
  kvstore_del(&testserver.store, "MYKEY1");

  kvserver_rebuild_state(&testserver);

  /* Check that server completed the transaction properly */
  reqmsg.type = GETREQ;
  reqmsg.key = "MYKEY1";
  kvserver_handle_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, GETRESP);
  ASSERT_STRING_EQUAL(respmsg.key, "MYKEY1");
  ASSERT_STRING_EQUAL(respmsg.value, "MYVALUE1");
  return 1;
}

void dummy_registration_handle(kvserver_t *server, int sockfd, void *extra) {
  kvmessage_t *register_msg, respmsg;
  pthread_mutex_lock(&kvserver_tpc_lock);
  register_msg = kvmessage_parse(sockfd);
  if (register_msg && register_msg->type == REGISTER
      && strcmp(register_msg->key, KVSERVER_TPC_HOSTNAME) == 0
      && atoi(register_msg->value) == KVSERVER_TPC_PORT)
    synch = 1;
  else
    synch = 0;

  memset(&respmsg, 0, sizeof(kvmessage_t));
  respmsg.type = RESP;
  respmsg.message = MSG_SUCCESS;
  kvmessage_send(&respmsg, sockfd);

  completed = 1;
  pthread_cond_signal(&kvserver_tpc_cond);
  pthread_mutex_unlock(&kvserver_tpc_lock);
  free(register_msg);
}

void *kvserver_tpc_test_connect_thread(void *aux) {
  int sockfd;
  struct sockaddr_in serv_addr;
  struct hostent *server;
  sockfd = socket(AF_INET, SOCK_STREAM, 0);
  server = gethostbyname("localhost");
  bzero((char *) &serv_addr, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  bcopy((char *)server->h_addr,
        (char *)&serv_addr.sin_addr.s_addr,
        server->h_length);
  serv_addr.sin_port = htons(KVSERVER_TPC_PORT);
  connect(sockfd,(struct sockaddr *) &serv_addr, sizeof(serv_addr));

  kvserver_register_master(&testserver, sockfd);

  shutdown(sockfd, SHUT_RDWR);
  close(sockfd);
  return 0;
}

void kvserver_tpc_test_connect() {
  pthread_t thread;
  pthread_create(&thread, NULL, &kvserver_tpc_test_connect_thread, NULL);
}

void *kvserver_tpc_server_runner(void *callback) {
  socket_server.master = 0;
  socket_server.max_threads = 2;
  memcpy(&socket_server.kvserver, &testserver, sizeof(kvserver_t));
  socket_server.kvserver.handle = dummy_registration_handle;
  server_run("slaveserver", KVSERVER_TPC_PORT, &socket_server, (callback_t) callback);
  return NULL;
}

int kvserver_tpc_registration(void) {
  int pass;
  pthread_t runner;
  completed = 0;
  pthread_create(&runner, NULL, &kvserver_tpc_server_runner, kvserver_tpc_test_connect);
  pthread_mutex_lock(&kvserver_tpc_lock);
  while (completed == 0)
    pthread_cond_wait(&kvserver_tpc_cond, &kvserver_tpc_lock);
  pass = (synch == 1);
  pthread_mutex_unlock(&kvserver_tpc_lock);
  server_stop(&socket_server);
  ASSERT_TRUE(pass);
  return 1;
}


test_info_t kvserver_tpc_tests[] = {
  {"Valid PUT request", kvserver_tpc_put_commit},
  {"Valid PUT request followed by multiple COMMITs", kvserver_tpc_put_multiple_commits},
  {"Valid PUT request followed by an ABORT", kvserver_tpc_put_abort},
  {"Invalid PUT request", kvserver_tpc_put_invalid},
  {"Valid DEL request", kvserver_tpc_del_commit},
  {"Invalid DEL request (key length)", kvserver_tpc_del_invalid_keylen},
  {"Valid DEL request followed by a PUT before COMMIT (not allowed)",
    kvserver_tpc_concurrent_del_put},
  {"Rebuild from a TPCLog with just a single PUT entry",
    kvserver_tpc_rebuild_single_put},
  {"Rebuild from a TPCLog with multiple entries and a final DEL",
    kvserver_tpc_rebuild_del},
  {"Rebuild from a TPCLog with an ABORT", kvserver_tpc_rebuild_abort},
  {"Rebuild from a TPCLog with PUT transaction ending in COMMIT, ensure that "
    "transaction is completed", kvserver_tpc_rebuild_put_commit},
  {"Rebuild from a TPCLog with transactions ending in multiple COMMITs",
    kvserver_tpc_rebuild_multiple_commits},
  {"KVServer registering with master", kvserver_tpc_registration},
  NULL_TEST_INFO
};

suite_info_t kvserver_tpc_suite = {"KVServer TPC Tests", kvserver_tpc_test_init,
  kvserver_tpc_test_clean, kvserver_tpc_tests};

