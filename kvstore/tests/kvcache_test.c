#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include "kvcache.h"
#include "kvconstants.h"
#include "tester.h"

kvcache_t testcache;

int kvcache_test_init(void) {
  kvcache_init(&testcache, 2, 2);
  return 0;
}

int kvcache_simple_put_get_single(void) {
  char *retval;
  int ret;
  ret = kvcache_put(&testcache, "mykey", "myvalue");
  ret += kvcache_get(&testcache, "mykey", &retval);
  ASSERT_PTR_NOT_NULL(retval);
  ASSERT_STRING_EQUAL(retval, "myvalue");
  ASSERT_EQUAL(ret, 0);
  free(retval);
  return 1;
}

int kvcache_simple_put_get_multiple(void) {
  char *retval;
  int ret;
  ret = kvcache_put(&testcache, "mykey1", "myvalue1");
  ret += kvcache_put(&testcache, "mykey2", "myvalue2");
  ret += kvcache_put(&testcache, "mykey3", "myvalue3");
  ret += kvcache_put(&testcache, "mykey4", "myvalue4");
  ret += kvcache_get(&testcache, "mykey1", &retval);
  ASSERT_PTR_NOT_NULL(retval);
  ASSERT_STRING_EQUAL(retval, "myvalue1");
  free(retval);
  ret += kvcache_get(&testcache, "mykey2", &retval);
  ASSERT_STRING_EQUAL(retval, "myvalue2");
  free(retval);
  ret += kvcache_get(&testcache, "mykey3", &retval);
  ASSERT_STRING_EQUAL(retval, "myvalue3");
  free(retval);
  ret += kvcache_get(&testcache, "mykey4", &retval);
  ASSERT_STRING_EQUAL(retval, "myvalue4");
  free(retval);
  ASSERT_EQUAL(ret, 0);
  return 1;
}

int kvcache_get_invalid_key(void) {
  char *retval = NULL;
  int ret;
  ret = kvcache_get(&testcache, "mykey", &retval);
  ASSERT_EQUAL(ret, ERRNOKEY);
  ASSERT_PTR_NULL(retval);
  return 1;
}

int kvcache_del_simple(void) {
  char *retval = NULL;
  int ret;
  ret = kvcache_put(&testcache, "mykey1", "myvalue1");
  ret += kvcache_put(&testcache, "mykey2", "myvalue2");
  ret += kvcache_del(&testcache, "mykey1");
  ret += kvcache_get(&testcache, "mykey2", &retval);
  ASSERT_PTR_NOT_NULL(retval);
  ASSERT_STRING_EQUAL(retval, "myvalue2");
  ASSERT_EQUAL(ret, 0);
  free(retval);
  retval = NULL;
  ret = kvcache_get(&testcache, "mykey1", &retval);
  ASSERT_PTR_NULL(retval);
  ASSERT_EQUAL(ret, ERRNOKEY);
  return 1;
}

int kvcache_set_locks(void) {
  pthread_rwlock_t *l1, *l2, *l3, *l4, *l5, *l6;
  kvcache_init(&testcache, 3, 3);
  kvcache_put(&testcache, "mykey1", "myvalue1");
  kvcache_put(&testcache, "mykey2", "myvalue2");
  kvcache_put(&testcache, "mykey3", "myvalue3");
  kvcache_put(&testcache, "mykey4", "myvalue4");
  kvcache_put(&testcache, "mykey5", "myvalue5");
  kvcache_put(&testcache, "mykey6", "myvalue6");
  l1 = kvcache_getlock(&testcache, "mykey1");
  l2 = kvcache_getlock(&testcache, "mykey2");
  l3 = kvcache_getlock(&testcache, "mykey3");
  l4 = kvcache_getlock(&testcache, "mykey4");
  l5 = kvcache_getlock(&testcache, "mykey5");
  l6 = kvcache_getlock(&testcache, "mykey6");
  ASSERT_NOT_EQUAL(l1, l2);
  ASSERT_NOT_EQUAL(l1, l3);
  ASSERT_EQUAL(l1, l4);
  ASSERT_NOT_EQUAL(l3, l5);
  ASSERT_EQUAL(l3, l6);
  ASSERT_EQUAL(l5, l2);
  ASSERT_NOT_EQUAL(l6, l2);
  return 1;
}


test_info_t kvcache_tests[] = {
  {"Simple PUT and GET of a single value", kvcache_simple_put_get_single},
  {"Simple PUT and GET of multiple values, filling to capacity",
    kvcache_simple_put_get_multiple},
  {"GET with invalid key", kvcache_get_invalid_key},
  {"Simple DEL test", kvcache_del_simple},
  {"Testing that locks are same for keys in same set, diff for keys in "
    "diff sets", kvcache_set_locks},
  NULL_TEST_INFO
};

suite_info_t kvcache_suite = {"KVCache Tests", kvcache_test_init, NULL,
  kvcache_tests};
