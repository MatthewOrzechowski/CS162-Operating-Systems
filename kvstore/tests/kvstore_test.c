#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "kvstore.h"
#include "tester.h"

#define KVSTORE_DIRNAME "kvstore-test"

kvstore_t teststore;

/* Deletes all current entries in the store and removes the store directory. */
int kvstore_test_clean(void) {
  return kvstore_clean(&teststore);
}

int kvstore_test_init(void) {
  return kvstore_init(&teststore, KVSTORE_DIRNAME);
}

int kvstore_del_simple(void) {
  char *retval;
  int ret;
  ret = kvstore_put(&teststore, "key", "val");
  ret += kvstore_get(&teststore, "key", &retval);
  ASSERT_STRING_EQUAL(retval, "val");
  free(retval);
  ret += kvstore_del(&teststore, "key");
  ASSERT_EQUAL(ret, 0);
  retval = NULL;
  ret = kvstore_get(&teststore, "key", &retval);
  ASSERT_EQUAL(ret, ERRNOKEY);
  ASSERT_PTR_NULL(retval);
  return 1;
}

int kvstore_get_put_with_hash_conflicts(void) {
  /* hash("abD") == hash("aae") == hash("ac#") */
  char *retval, *key1 = "abD", *key2 = "aae", *key3 = "ac#";
  int ret;
  ret = kvstore_put(&teststore, key1, "value1");
  ret += kvstore_put(&teststore, key2, "value2");
  ret += kvstore_put(&teststore, key3, "value3");
  ret += kvstore_get(&teststore, key1, &retval);
  ASSERT_STRING_EQUAL(retval, "value1");
  free(retval);
  ret += kvstore_get(&teststore, key2, &retval);
  ASSERT_STRING_EQUAL(retval, "value2");
  free(retval);
  ret += kvstore_get(&teststore, key3, &retval);
  ASSERT_STRING_EQUAL(retval, "value3");
  free(retval);
  /* Clean the store and do it again in a different order to ensure that
     the above success wasn't just because of a lucky ordering. */
  kvstore_clean(&teststore);
  kvstore_init(&teststore, KVSTORE_DIRNAME);
  ret = kvstore_put(&teststore, key2, "value2");
  ret += kvstore_put(&teststore, key3, "value3");
  ret += kvstore_put(&teststore, key1, "value1");
  ret += kvstore_get(&teststore, key1, &retval);
  ASSERT_STRING_EQUAL(retval, "value1");
  free(retval);
  ret += kvstore_get(&teststore, key2, &retval);
  ASSERT_STRING_EQUAL(retval, "value2");
  free(retval);
  ret += kvstore_get(&teststore, key3, &retval);
  ASSERT_STRING_EQUAL(retval, "value3");
  free(retval);
  ASSERT_EQUAL(ret, 0);
  return 1;
}

int kvstore_del_hash_conflicts(void) {
  /* hash("abD") == hash("aae") == hash("ac#") */
  char *retval = NULL, *key1 = "abD", *key2 = "aae", *key3 = "ac#";
  int ret;
  ret = kvstore_put(&teststore, key1, "value1");
  ret += kvstore_put(&teststore, key2, "value2");
  ret += kvstore_put(&teststore, key3, "value3");
  ret += kvstore_del(&teststore, key2);
  ASSERT_EQUAL(ret, 0);
  ret = kvstore_get(&teststore, key2, &retval);
  ASSERT_PTR_NULL(retval);
  ASSERT_EQUAL(ret, ERRNOKEY);
  ret = kvstore_get(&teststore, key1, &retval);
  ASSERT_STRING_EQUAL(retval, "value1");
  free(retval);
  ret += kvstore_get(&teststore, key3, &retval);
  ASSERT_STRING_EQUAL(retval, "value3");
  free(retval);
  ASSERT_EQUAL(ret, 0);
  /* Clean store and do operations again with a different insertion order to
   * help ensure that success wasn't due to a lucky ordering. */
  kvstore_clean(&teststore);
  kvstore_init(&teststore, KVSTORE_DIRNAME);
  ret = kvstore_put(&teststore, key2, "value2");
  ret += kvstore_put(&teststore, key1, "value1");
  ret += kvstore_put(&teststore, key3, "value3");
  ret += kvstore_del(&teststore, key2);
  ASSERT_EQUAL(ret, 0);
  retval = NULL;
  ret = kvstore_get(&teststore, key2, &retval);
  ASSERT_PTR_NULL(retval);
  ASSERT_EQUAL(ret, ERRNOKEY);
  ret = kvstore_get(&teststore, key1, &retval);
  ASSERT_STRING_EQUAL(retval, "value1");
  free(retval);
  ret += kvstore_get(&teststore, key3, &retval);
  ASSERT_STRING_EQUAL(retval, "value3");
  free(retval);
  ASSERT_EQUAL(ret, 0);
  return 1;
}

int kvstore_del_no_key(void) {
  int ret;
  ret = kvstore_del(&teststore, "nonexistent key");
  ASSERT_EQUAL(ret, ERRNOKEY);
  return 1;
}

int kvstore_put_get_blank_value(void) {
  char *retval;
  int ret;
  ret = kvstore_put(&teststore, "valid key", "");
  ret += kvstore_get(&teststore, "valid key", &retval);
  ASSERT_STRING_EQUAL(retval, "");
  ASSERT_EQUAL(ret, 0);
  free(retval);
  return 1;
}

int kvstore_get_no_key(void) {
  char *retval = NULL;
  int ret;
  kvstore_test_init();
  ret = kvstore_get(&teststore, "NONEXISTENT KEY", &retval);
  ASSERT_EQUAL(ret, ERRNOKEY);
  ASSERT_PTR_NULL(retval);
  return 1;
}

int kvstore_put_overwrite(void) {
  char *retval;
  int ret;
  ret = kvstore_put(&teststore, "mykey", "initial value");
  ret += kvstore_put(&teststore, "mykey", "updated value");
  ret += kvstore_get(&teststore, "mykey", &retval);
  ASSERT_STRING_EQUAL(retval, "updated value");
  free(retval);
  ASSERT_EQUAL(ret, 0);
  return 1;
}

int kvstore_get_oversized_key(void) {
  char *retval = NULL, oversizedkey[MAX_KEYLEN + 2];
  int ret, i;
  strcpy(oversizedkey, "a");
  for (i = 1; i < MAX_KEYLEN + 1; i++)
    strcat(oversizedkey, "a");
  ret = kvstore_get(&teststore, oversizedkey, &retval);
  ASSERT_EQUAL(ret, ERRKEYLEN);
  ASSERT_PTR_NULL(retval);
  return 1;
}

int kvstore_put_oversized_fields(void) {
  char oversizedkey[MAX_KEYLEN + 2], oversizedvalue[MAX_VALLEN + 2];
  int ret, i;
  strcpy(oversizedkey, "a");
  strcpy(oversizedvalue, "v");
  for (i = 1; i < MAX_KEYLEN + 1; i++)
    strcat(oversizedkey, "a");
  for (i = 1; i < MAX_VALLEN + 1; i++)
    strcat(oversizedvalue, "v");
  ret = kvstore_put(&teststore, oversizedkey, "normal value");
  ASSERT_EQUAL(ret, ERRKEYLEN);
  ret = kvstore_put(&teststore, "normal key", oversizedvalue);
  ASSERT_EQUAL(ret, ERRVALLEN);
  return 1;
}

int kvstore_get_put_uninitialized(void) {
  char *retval;
  int ret;
  kvstore_clean(&teststore);
  ret = kvstore_get(&teststore, "KEY", &retval);
  ASSERT_EQUAL(ret, ERRFILACCESS);
  ret = kvstore_put(&teststore, "KEY", "VALUE");
  ASSERT_EQUAL(ret, ERRFILACCESS);
  return 1;
}

int kvstore_single_put_get(void) {
  char *retval;
  int ret;
  ret = kvstore_put(&teststore, "MYKEY", "MYVALUE");
  ASSERT_EQUAL(ret, 0);
  ret = kvstore_get(&teststore, "MYKEY", &retval);
  ASSERT_EQUAL(ret, 0);
  ASSERT_STRING_EQUAL(retval, "MYVALUE");
  free(retval);
  return 1;
}

int kvstore_multiple_put_get(void) {
  char *retval;
  int ret;
  ret = kvstore_put(&teststore, "KEY1", "VALUE1");
  ret += kvstore_put(&teststore, "KEY2", "VALUE2");
  ret += kvstore_put(&teststore, "KEY3", "VALUE3");
  ret += kvstore_get(&teststore, "KEY1", &retval);
  ASSERT_STRING_EQUAL(retval, "VALUE1");
  free(retval);
  ret += kvstore_get(&teststore, "KEY2", &retval);
  ASSERT_STRING_EQUAL(retval, "VALUE2");
  free(retval);
  ret += kvstore_get(&teststore, "KEY3", &retval);
  ASSERT_STRING_EQUAL(retval, "VALUE3");
  free(retval);
  ASSERT_EQUAL(ret, 0);
  return 1;
}

test_info_t kvstore_tests[] = {
  {"Simple PUT and GET of a single value", kvstore_single_put_get},
  {"Simple PUT and GET of multiple values", kvstore_multiple_put_get},
  {"GET when there is no valid key", kvstore_get_no_key},
  {"PUT and GET when the kvstore has not been initialized",
    kvstore_get_put_uninitialized},
  {"GET on an oversized key", kvstore_get_oversized_key},
  {"PUT on a key that already exists in the store (should overwrite)",
    kvstore_put_overwrite},
  {"PUT on an oversized key or value", kvstore_put_oversized_fields},
  {"PUT and GET with a blank (\"\") value", kvstore_put_get_blank_value},
  {"PUT and GET on keys which have hash conflicts",
    kvstore_get_put_with_hash_conflicts},
  {"Simple DEL on a value", kvstore_del_simple},
  {"DEL on a key that does not exist", kvstore_del_no_key},
  {"DEL on keys which have hash conflicts", kvstore_del_hash_conflicts},
  NULL_TEST_INFO
};

suite_info_t kvstore_suite = {"KVStore Tests", kvstore_test_init,
  kvstore_test_clean, kvstore_tests};
