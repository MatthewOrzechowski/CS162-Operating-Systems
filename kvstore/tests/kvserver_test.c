#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <stdbool.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <pthread.h>
#include "socket_server.h"
#include "kvmessage.h"
#include "kvserver.h"
#include "tester.h"

#define KVSERVER_HOSTNAME "localhost"
#define KVSERVER_PORT 8162
#define KVSERVER_DIRNAME "kvserver-test"
/* SLEEP_TIME is the number of milliseconds to sleep during concurrency tests
 * to ensure that the new thread has a chance to run. If tests are failing
 * intermittently, increase this value. */
#define SLEEP_TIME 10

kvserver_t testserver;
kvmessage_t reqmsg, respmsg;

/* Used for synchronizing some of the concurrency tests. */
int synch;

/* Deletes all current entries in the store and removes the store directory. */
int kvserver_test_clean(void) {
  return kvserver_clean(&testserver);
}

int kvserver_test_init(void) {
  memset(&reqmsg, 0, sizeof(kvmessage_t));
  memset(&respmsg, 0, sizeof(kvmessage_t));
  kvserver_init(&testserver, KVSERVER_DIRNAME, 4, 4, 1, KVSERVER_HOSTNAME,
      KVSERVER_PORT, false);
  return 0;
}

int kvserver_single_put_get(void) {
  reqmsg.type = PUTREQ;
  reqmsg.key = "MYKEY";
  reqmsg.value = "MYVALUE";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, MSG_SUCCESS);
  reqmsg.type = GETREQ;
  reqmsg.key = "MYKEY";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, GETRESP);
  ASSERT_STRING_EQUAL(respmsg.key, "MYKEY");
  ASSERT_STRING_EQUAL(respmsg.value, "MYVALUE");
  return 1;
}

int kvserver_del_simple(void) {
  reqmsg.type = PUTREQ;
  reqmsg.key = "MYKEY";
  reqmsg.value = "MYVALUE";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, MSG_SUCCESS);

  reqmsg.type = DELREQ;
  reqmsg.key = "MYKEY";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, MSG_SUCCESS);

  reqmsg.type = GETREQ;
  reqmsg.key = "MYKEY";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, ERRMSG_NO_KEY);
  return 1;
}

int kvserver_get_no_key(void) {
  reqmsg.type = PUTREQ;
  reqmsg.key = "MYKEY";
  reqmsg.value = "MYVALUE";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, MSG_SUCCESS);

  reqmsg.type = GETREQ;
  reqmsg.key = "NOT MYKEY";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, ERRMSG_NO_KEY);
  return 1;
}

int kvserver_get_oversized_key(void) {
  char oversized_key[MAX_KEYLEN + 10];
  int i;
  reqmsg.type = GETREQ;
  for (i = 0; i < MAX_KEYLEN + 8; i++)
    oversized_key[i] = 'a';
  oversized_key[i] = '\0';
  reqmsg.key = oversized_key;
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, ERRMSG_KEY_LEN);
  return 1;
}

int kvserver_put_oversized_fields(void) {
  char oversized_key[MAX_KEYLEN + 10], oversized_value[MAX_VALLEN + 10];
  int i;
  for (i = 0; i < MAX_KEYLEN + 8; i++)
    oversized_key[i] = 'a';
  oversized_key[i] = '\0';
  for (i = 0; i < MAX_VALLEN + 8; i++)
    oversized_value[i] = 'a';
  oversized_value[i] = '\0';

  reqmsg.type = PUTREQ;
  reqmsg.key = "MYKEY";
  reqmsg.value = oversized_value;
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, ERRMSG_VAL_LEN);

  reqmsg.key = oversized_key;
  reqmsg.value = "MYVALUE";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, ERRMSG_KEY_LEN);
  return 1;
}

int kvserver_multiple_put_get(void) {
  reqmsg.type = PUTREQ;
  reqmsg.key = "MYKEY1";
  reqmsg.value = "MYVALUE1";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, MSG_SUCCESS);

  reqmsg.key = "MYKEY2";
  reqmsg.value = "MYVALUE2";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, MSG_SUCCESS);

  reqmsg.key = "MYKEY3";
  reqmsg.value = "MYVALUE3";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, MSG_SUCCESS);

  reqmsg.type = GETREQ;
  reqmsg.key = "MYKEY1";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, GETRESP);
  ASSERT_STRING_EQUAL(respmsg.key, "MYKEY1");
  ASSERT_STRING_EQUAL(respmsg.value, "MYVALUE1");

  reqmsg.type = GETREQ;
  reqmsg.key = "MYKEY3";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, GETRESP);
  ASSERT_STRING_EQUAL(respmsg.key, "MYKEY3");
  ASSERT_STRING_EQUAL(respmsg.value, "MYVALUE3");

  reqmsg.type = GETREQ;
  reqmsg.key = "MYKEY2";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, GETRESP);
  ASSERT_STRING_EQUAL(respmsg.key, "MYKEY2");
  ASSERT_STRING_EQUAL(respmsg.value, "MYVALUE2");
  return 1;
}

int kvserver_use_cache(void) {
  reqmsg.type = PUTREQ;
  reqmsg.key = "MYKEY";
  reqmsg.value = "MYVALUE";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, MSG_SUCCESS);

  kvserver_clean(&testserver);
  /* Doing a request after deleting the KVStore directory should normally
   * result in an error, but since the key should be in the cache, the store
   * should never be touched. */
  reqmsg.type = GETREQ;
  reqmsg.key = "MYKEY";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, GETRESP);
  ASSERT_STRING_EQUAL(respmsg.key, "MYKEY");
  ASSERT_STRING_EQUAL(respmsg.value, "MYVALUE");
  return 1;
}

int kvserver_get_fills_cache(void) {
  kvserver_init(&testserver, KVSERVER_DIRNAME, 1, 2, 1, KVSERVER_HOSTNAME,
      KVSERVER_PORT, false);
  reqmsg.type = PUTREQ;
  reqmsg.key = "MYKEY1";
  reqmsg.value = "MYVALUE1";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, MSG_SUCCESS);

  /* Overwrite MYKEY1 in the cache */
  reqmsg.key = "MYKEY2";
  reqmsg.value = "MYVALUE2";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, MSG_SUCCESS);
  reqmsg.key = "MYKEY3";
  reqmsg.value = "MYVALUE3";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, MSG_SUCCESS);

  /* Should bring MYKEY1 back into the cache */
  reqmsg.type = GETREQ;
  reqmsg.key = "MYKEY1";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, GETRESP);
  ASSERT_STRING_EQUAL(respmsg.key, "MYKEY1");
  ASSERT_STRING_EQUAL(respmsg.value, "MYVALUE1");

  kvserver_clean(&testserver);
  /* Doing a request after deleting the KVStore directory should normally
   * result in an error, but since the key should be in the cache, the store
   * should never be touched. */
  reqmsg.type = GETREQ;
  reqmsg.key = "MYKEY1";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, GETRESP);
  ASSERT_STRING_EQUAL(respmsg.key, "MYKEY1");
  ASSERT_STRING_EQUAL(respmsg.value, "MYVALUE1");
  return 1;
}

/* Attempts to submit the current request message and then set SYNCH variable
 * to 1 to indicate that the request completed. */
void *kvserver_concurrent_helper(void *aux) {
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  synch = 1;
  pthread_exit(0);
}

int kvserver_cache_concurrent_puts(void) {
  pthread_rwlock_t *cachelock;
  pthread_t thread;

  reqmsg.type = PUTREQ;
  reqmsg.key = "MYKEY";
  reqmsg.value = "MYVALUE";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, MSG_SUCCESS);

  synch = 0;
  reqmsg.value = "NEW MYVALUE";

  cachelock = kvcache_getlock(&testserver.cache, "MYKEY");
  pthread_rwlock_rdlock(cachelock);
  pthread_create(&thread, NULL, kvserver_concurrent_helper, NULL);
  usleep(1000 * SLEEP_TIME); /* Give the put request a chance to run. */
  ASSERT_EQUAL(synch, 0); /* Ensure the put request didn't complete yet. */
  pthread_rwlock_unlock(cachelock);
  pthread_join(thread, NULL);
  ASSERT_EQUAL(synch, 1); /* Ensure that the put request was able to run after the lock was released. */
  return 1;
}

int kvserver_cache_concurrent_gets_rdlock(void) {
  pthread_rwlock_t *cachelock;
  pthread_t thread;

  reqmsg.type = PUTREQ;
  reqmsg.key = "MYKEY";
  reqmsg.value = "MYVALUE";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, MSG_SUCCESS);

  synch = 0;
  reqmsg.type = GETREQ;

  cachelock = kvcache_getlock(&testserver.cache, "MYKEY");
  pthread_rwlock_rdlock(cachelock);
  pthread_create(&thread, NULL, kvserver_concurrent_helper, NULL);
  usleep(1000 * SLEEP_TIME); /* Give the get request a chance to run. */
  ASSERT_EQUAL(synch, 1); /* Ensure the get request was able to complete already. */
  pthread_rwlock_unlock(cachelock);
  pthread_join(thread, NULL);
  return 1;
}

int kvserver_cache_concurrent_get_cache_writes(void) {
  pthread_rwlock_t *cachelock;
  pthread_t thread;
  kvserver_init(&testserver, KVSERVER_DIRNAME, 1, 2, 1, KVSERVER_HOSTNAME, KVSERVER_PORT, false);
  reqmsg.type = PUTREQ;
  reqmsg.key = "MYKEY1";
  reqmsg.value = "MYVALUE1";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, MSG_SUCCESS);

/* Clear MYKEY1 from the cache */
  reqmsg.key = "MYKEY2";
  reqmsg.value = "MYVALUE2";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg); 
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, MSG_SUCCESS);
  reqmsg.key = "MYKEY3";
  reqmsg.value = "MYVALUE3";
  kvserver_handle_no_tpc(&testserver, &reqmsg, &respmsg); 
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, MSG_SUCCESS);

  synch = 0;
  reqmsg.type = GETREQ;
  reqmsg.key = "MYKEY1";

  cachelock = kvcache_getlock(&testserver.cache, "MYKEY1");
  pthread_rwlock_rdlock(cachelock);
  pthread_create(&thread, NULL, kvserver_concurrent_helper, NULL);
  usleep(1000 * SLEEP_TIME); /* Give the get request a chance to run. */
  ASSERT_EQUAL(synch, 0); /* Ensure the get request didn't complete yet. */
  pthread_rwlock_unlock(cachelock);
  pthread_join(thread, NULL);
  ASSERT_EQUAL(synch, 1); /* Ensure that the get request was able to run after the lock was released. */
  return 1;
}


test_info_t kvserver_tests[] = {
  {"Simple PUT and GET of a single value", kvserver_single_put_get},
  {"Simple PUT and GET of multiple values", kvserver_multiple_put_get},
  {"GET when there is no valid key", kvserver_get_no_key},
  {"GET on an oversized key", kvserver_get_oversized_key},
  {"GET requests fill the cache", kvserver_get_fills_cache},
  {"PUT on an oversized key or value", kvserver_put_oversized_fields},
  {"Simple DEL on a value", kvserver_del_simple},
  {"PUT request cannot complete when a lock is held on cacheset",
    kvserver_cache_concurrent_puts},
  {"GET request can complete when a read lock is held on cacheset",
    kvserver_cache_concurrent_gets_rdlock},
  {"GET request cannot complete when a read lock is held on cacheset and the "
    "cache must be filled", kvserver_cache_concurrent_get_cache_writes},
  NULL_TEST_INFO
};

suite_info_t kvserver_suite = {"KVserver Tests (No TPC)", kvserver_test_init,
  kvserver_test_clean, kvserver_tests};
