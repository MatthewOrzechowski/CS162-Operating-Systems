#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <pthread.h>
#include <semaphore.h>
#include "socket_server.h"
#include "tester.h"

#define SOCKET_SERVER_PORT 8162
#define SOCKET_SERVER_HOST "localhost"

int synch, server_running, concurrent, complete;
pthread_mutex_t socket_server_test_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t socket_server_test_cond = PTHREAD_COND_INITIALIZER,
  socket_server_completion_cond = PTHREAD_COND_INITIALIZER;
server_t testserver;

int socket_server_test_init(void) {
  synch = 0;
  complete = 0;
  concurrent = 0;
  server_running = 0;
  return 0;
}

void socket_server_request_handler(kvserver_t *server, int sockfd, void *extra) {
  pthread_mutex_lock(&socket_server_test_lock);
  concurrent++;
  if (concurrent == 20) {
    complete = 1;
    pthread_cond_signal(&socket_server_completion_cond);
  }
  while (concurrent < 20)
    pthread_cond_wait(&socket_server_test_cond, &socket_server_test_lock);
  pthread_mutex_unlock(&socket_server_test_lock);
}

void *socket_server_request_thread(void* aux) {
  connect_to(SOCKET_SERVER_HOST, SOCKET_SERVER_PORT, 3);
  return NULL;
}

void socket_server_run_callback(void* aux) {
  pthread_mutex_lock(&socket_server_test_lock);
  server_running = 1;
  pthread_cond_signal(&socket_server_test_cond);
  pthread_mutex_unlock(&socket_server_test_lock);
}

void *socket_server_run_thread(void* aux) {
  server_run(SOCKET_SERVER_HOST, SOCKET_SERVER_PORT, &testserver, socket_server_run_callback);
  return NULL;
}

void *socket_server_timeout_thread(void* aux) {
  sleep(3);

  pthread_mutex_lock(&socket_server_test_lock);
  if (concurrent < 20) {
    synch = 1;
    complete = 1;
    pthread_cond_signal(&socket_server_completion_cond);
  }
  pthread_mutex_unlock(&socket_server_test_lock);
  return NULL;
}

int socket_server_multiple_test(void) {
  pthread_t server_thread, timeout_thread, request_threads[20];

  testserver.master = 0;
  testserver.max_threads = 20;
  testserver.kvserver.handle = &socket_server_request_handler;

  pthread_create(&server_thread, NULL, socket_server_run_thread, NULL);
  pthread_mutex_lock(&socket_server_test_lock);
  while (!server_running)
    pthread_cond_wait(&socket_server_test_cond, &socket_server_test_lock);
  pthread_mutex_unlock(&socket_server_test_lock);
  for (int i = 0; i < 20; i++)
    pthread_create(&request_threads[i], NULL, socket_server_request_thread, NULL);

  pthread_create(&timeout_thread, NULL, socket_server_timeout_thread, NULL);

  pthread_mutex_lock(&socket_server_test_lock);
  while (!complete) 
    pthread_cond_wait(&socket_server_completion_cond, &socket_server_test_lock);
  pthread_cond_broadcast(&socket_server_test_cond);
  pthread_mutex_unlock(&socket_server_test_lock);

  for (int i = 0; i < 20; i++)
    pthread_join(request_threads[i], NULL);

  server_stop(&testserver);
  ASSERT_EQUAL(synch, 0);
  return 1;
}

test_info_t socket_server_tests[] = {
  {"Tests that multiple requests can be handled simultaneously", socket_server_multiple_test},
  NULL_TEST_INFO
};

suite_info_t socket_server_suite = {"Socket Server Tests", socket_server_test_init, NULL, socket_server_tests};
