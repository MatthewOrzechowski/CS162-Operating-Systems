#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <stdbool.h>
#include <dirent.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include "kvmessage.h"
#include "kvserver.h"
#include "tester.h"
#include "socket_server.h"

#define KVSERVER_HOSTNAME "server-client.com"
#define KVSERVER_PORT 8162
#define KVSERVER_DIRNAME "kvserver-test"
/* SLEEP_TIME is the number of milliseconds to sleep during concurrency tests
 * to ensure that the new thread has a chance to run. If tests are failing
 * intermittently, increase this value. */
#define SLEEP_TIME 5

int synch;
int synch_expect;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;

server_t server;

void dummy_handle(kvserver_t *server, int sockfd, void *extra)
{
  pthread_mutex_lock(&lock);
  synch += 1;
  if (synch == synch_expect) {
    pthread_cond_signal(&cond);
  }
  pthread_mutex_unlock(&lock);
}

void* test_connect(void* aux) {
  int sockfd, n;
  struct sockaddr_in serv_addr;
  struct hostent *server;
  sockfd = socket(AF_INET, SOCK_STREAM, 0);
  server = gethostbyname("localhost");
  bzero((char *) &serv_addr, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  bcopy((char *) server->h_addr,
      (char *) &serv_addr.sin_addr.s_addr,
      server->h_length);
  serv_addr.sin_port = htons(KVSERVER_PORT);
  if (connect(sockfd,(struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
    pthread_mutex_lock(&lock);
    synch -= 999;
    pthread_cond_signal(&cond);
    pthread_mutex_unlock(&lock);
    return NULL;
  }
  n = write(sockfd,"TEST",5);
  if (n < 0) {
    printf("ERROR writing to socket");
  }
  shutdown(sockfd, SHUT_RDWR);
  close(sockfd);
  return NULL;
}

void threaded_test_connect() {
  pthread_t workers[10];
  for (int i = 0; i < 10; i++) {
    pthread_create(&workers[i], NULL, &test_connect, NULL);
  }
  for (int i = 0; i < 10; i++) {
    pthread_join(workers[i], NULL);
  }
}

void serialized_test_connect() {
  for (int i = 0; i < 10; i++) {
    test_connect(NULL);
  }
}

void *server_runner(void *callback){
  server.master = 0;
  server.max_threads = 3;
  kvserver_init(&server.kvserver, "slave", 4, 4, 2, KVSERVER_HOSTNAME,
      KVSERVER_PORT, false);
  server.kvserver.handle = dummy_handle;
  server_run("slaveserver", KVSERVER_PORT, &server, (callback_t) callback);
  return NULL;
}

int kvserver_client_test_simple(void) {
  synch = 0;
  synch_expect = 1;
  int pass;
  pthread_t runner;
  pthread_create(&runner, NULL, &server_runner, test_connect);
  pthread_mutex_lock(&lock);
  pthread_cond_wait(&cond, &lock);
  pass = (synch == synch_expect);
  pthread_mutex_unlock(&lock);
  server_stop(&server);
  ASSERT(pass);
  return 1;
}

int kvserver_client_test_serial(void) {
  synch = 0;
  synch_expect = 10;
  int pass;
  pthread_t runner;
  pthread_create(&runner, NULL, &server_runner, serialized_test_connect);
  pthread_mutex_lock(&lock);
  pthread_cond_wait(&cond, &lock);
  pass = (synch == synch_expect);
  pthread_mutex_unlock(&lock);
  server_stop(&server);
  ASSERT(pass);
  return 1;
}

int kvserver_client_test_threaded(void) {
  synch = 0;
  synch_expect = 10;
  int pass;
  pthread_t runner;
  pthread_create(&runner, NULL, &server_runner, threaded_test_connect);
  pthread_mutex_lock(&lock);
  pthread_cond_wait(&cond, &lock);
  pass = (synch == synch_expect);
  pthread_mutex_unlock(&lock);
  server_stop(&server);
  ASSERT(pass);
  return 1;
}

test_info_t kvserver_client_tests[] = {
  {"Simple kvserver client test", kvserver_client_test_simple},
  {"Multiple client kvserver client test (serial)", kvserver_client_test_serial},
  {"Multiple client kvserver client test (threaded)", kvserver_client_test_threaded},
  NULL_TEST_INFO
};

suite_info_t kvserver_client_suite = {"KVServer Client Tests", NULL, NULL,
  kvserver_client_tests};
