#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <pthread.h>
#include "wq.h"
#include "tester.h"

int synch, has_run, completed;
pthread_mutex_t wq_test_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t wq_test_cond = PTHREAD_COND_INITIALIZER;
wq_t testwq;

bool item_received[20];

int wq_test_init(void) {
  synch = 0;
  completed = 0;
  has_run = 0;
  wq_init(&testwq);
  return 0;
}

void *wq_pop_test_thread_multiple(void* aux) {
  int item;

  item = (intptr_t) wq_pop(&testwq);
  pthread_mutex_lock(&wq_test_lock);
  if (item_received[item])
    synch = 1;
  item_received[item] = 1;
  completed++;
  if (completed == 20)
    pthread_cond_signal(&wq_test_cond);
  pthread_mutex_unlock(&wq_test_lock);
  return NULL;
}

int wq_wait_multiple_test(void) {
  pthread_t pop_threads[20];

  for (int i = 0; i < 20; i++)
    pthread_create(&pop_threads[i], NULL, wq_pop_test_thread_multiple, NULL);
  for (int i = 0; i < 20; i++)
    wq_push(&testwq, (void *) (intptr_t) i);

  pthread_mutex_lock(&wq_test_lock);
  while (completed < 20)
    pthread_cond_wait(&wq_test_cond, &wq_test_lock);
  pthread_mutex_unlock(&wq_test_lock);
  ASSERT_EQUAL(synch, 0);
  return 1;
}

void *wq_pop_test_thread_single(void* aux) {
  int item;

  pthread_mutex_lock(&wq_test_lock);
  has_run = 1;
  pthread_cond_signal(&wq_test_cond);
  pthread_mutex_unlock(&wq_test_lock);

  item = (intptr_t) wq_pop(&testwq);

  pthread_mutex_lock(&wq_test_lock);
  if (item != 162)
    synch = 1;
  completed = 1;
  pthread_cond_signal(&wq_test_cond);
  pthread_mutex_unlock(&wq_test_lock);
  return NULL;
}

int wq_wait_single_test(void) {
  pthread_t pop_thread;
  pthread_create(&pop_thread, NULL, wq_pop_test_thread_single, NULL);
  pthread_mutex_lock(&wq_test_lock);
  while (!has_run)
    pthread_cond_wait(&wq_test_cond, &wq_test_lock);
  pthread_mutex_unlock(&wq_test_lock);
  sleep(1); /* Give the other thread a chance to continue waiting for a bit */
  ASSERT_FALSE(completed);

  wq_push(&testwq, (void *) 162);
  pthread_mutex_lock(&wq_test_lock);
  while (!completed)
    pthread_cond_wait(&wq_test_cond, &wq_test_lock);
  pthread_mutex_unlock(&wq_test_lock);
  ASSERT_EQUAL(synch, 0);
  return 1;
}

test_info_t wq_tests[] = {
  {"Tests that a thread popping will wait until there is an item in the queue", wq_wait_single_test},
  {"Tests that multiple threads waiting will get one item each", wq_wait_multiple_test},
  NULL_TEST_INFO
};

suite_info_t wq_suite = {"WQ Tests", wq_test_init, NULL, wq_tests};
