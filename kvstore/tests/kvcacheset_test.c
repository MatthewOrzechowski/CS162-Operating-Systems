#include <stdlib.h>
#include "tester.h"
#include "kvcacheset.h"
#include "kvconstants.h"

kvcacheset_t testset;

int kvcacheset_test_init(void) {
  kvcacheset_init(&testset, 3);
  return 0;
}

int kvcacheset_simple_put_get_single(void) {
  char *retval;
  int ret;
  ret = kvcacheset_put(&testset, "mykey", "myvalue");
  ret += kvcacheset_get(&testset, "mykey", &retval);
  ASSERT_STRING_EQUAL(retval, "myvalue");
  ASSERT_EQUAL(ret, 0);
  free(retval);
  return 1;
}

int kvcacheset_simple_put_get_multiple(void) {
  char *retval;
  int ret;
  ret = kvcacheset_put(&testset, "mykey1", "myvalue1");
  ret += kvcacheset_put(&testset, "mykey2", "myvalue2");
  ret += kvcacheset_put(&testset, "mykey3", "myvalue3");
  ret += kvcacheset_get(&testset, "mykey1", &retval);
  ASSERT_STRING_EQUAL(retval, "myvalue1");
  free(retval);
  ret += kvcacheset_get(&testset, "mykey2", &retval);
  ASSERT_STRING_EQUAL(retval, "myvalue2");
  free(retval);
  ret += kvcacheset_get(&testset, "mykey3", &retval);
  ASSERT_STRING_EQUAL(retval, "myvalue3");
  free(retval);
  ASSERT_EQUAL(ret, 0);
  return 1;
}

int kvcacheset_del_simple(void) {
  char *retval = NULL;
  int ret;
  ret = kvcacheset_put(&testset, "mykey1", "myvalue1");
  ret += kvcacheset_put(&testset, "mykey2", "myvalue2");
  ret += kvcacheset_del(&testset, "mykey1");
  ret += kvcacheset_get(&testset, "mykey2", &retval);
  ASSERT_PTR_NOT_NULL(retval);
  ASSERT_STRING_EQUAL(retval, "myvalue2");
  ASSERT_EQUAL(ret, 0);
  if (retval != NULL)
    free(retval);
  retval = NULL;
  ret = kvcacheset_get(&testset, "mykey1", &retval);
  ASSERT_PTR_NULL(retval);
  ASSERT_EQUAL(ret, ERRNOKEY);
  return 1;
}

int kvcacheset_put_overwrite(void) {
  char *retval = NULL;
  int ret;
  ret = kvcacheset_put(&testset, "mykey", "initial value");
  ret += kvcacheset_put(&testset, "mykey", "updated value");
  ret += kvcacheset_get(&testset, "mykey", &retval);
  ASSERT_PTR_NOT_NULL(retval);
  ASSERT_STRING_EQUAL(retval, "updated value");
  ASSERT_EQUAL(ret, 0);
  return 1;
}

int kvcacheset_replacement_no_ref_bits(void) {
  char *retval = NULL;
  int ret;
  kvcacheset_put(&testset, "key1", "val1");
  kvcacheset_put(&testset, "key2", "val2");
  kvcacheset_put(&testset, "key3", "val3");
  kvcacheset_put(&testset, "key4", "val4");
  ret = kvcacheset_get(&testset, "key1", &retval);
  ASSERT_EQUAL(ret, ERRNOKEY);
  kvcacheset_get(&testset, "key2", &retval);
  ASSERT_STRING_EQUAL(retval, "val2");
  free(retval);
  kvcacheset_get(&testset, "key3", &retval);
  ASSERT_STRING_EQUAL(retval, "val3");
  free(retval);
  kvcacheset_get(&testset, "key4", &retval);
  ASSERT_STRING_EQUAL(retval, "val4");
  free(retval);
  return 1;
}

int kvcacheset_replacement_all_ref_bits(void) {
  char *retval = NULL;
  int ret;
  kvcacheset_put(&testset, "key1", "val1");
  kvcacheset_put(&testset, "key2", "val2");
  kvcacheset_put(&testset, "key3", "val3");
  kvcacheset_get(&testset, "key2", &retval);
  free(retval);
  kvcacheset_get(&testset, "key3", &retval);
  free(retval);
  kvcacheset_put(&testset, "key1", "val1new");
  kvcacheset_put(&testset, "key4", "val4");
  ret = kvcacheset_get(&testset, "key1", &retval);
  ASSERT_EQUAL(ret, ERRNOKEY);
  kvcacheset_get(&testset, "key2", &retval);
  ASSERT_STRING_EQUAL(retval, "val2");
  free(retval);
  kvcacheset_get(&testset, "key3", &retval);
  ASSERT_STRING_EQUAL(retval, "val3");
  free(retval);
  kvcacheset_get(&testset, "key4", &retval);
  ASSERT_STRING_EQUAL(retval, "val4");
  free(retval);
  return 1;
}

int kvcacheset_clear_all(void) {
  char *retval = NULL;
  int ret;
  kvcacheset_put(&testset, "key1", "val1");
  kvcacheset_put(&testset, "key2", "val2");
  kvcacheset_put(&testset, "key3", "val3");
  /* Ensure everything got put into the cache */
  kvcacheset_get(&testset, "key1", &retval);
  ASSERT_PTR_NOT_NULL(retval); 
  kvcacheset_clear(&testset);

  retval = NULL;
  ret = kvcacheset_get(&testset, "key1", &retval);
  ASSERT_PTR_NULL(retval); 
  ASSERT_EQUAL(ret, ERRNOKEY);
  ret = kvcacheset_get(&testset, "key2", &retval);
  ASSERT_PTR_NULL(retval); 
  ASSERT_EQUAL(ret, ERRNOKEY);
  ret = kvcacheset_get(&testset, "key3", &retval);
  ASSERT_PTR_NULL(retval); 
  ASSERT_EQUAL(ret, ERRNOKEY);
  return 1;
}


test_info_t kvcacheset_tests[] = {
  {"Simple PUT and GET of a single value", kvcacheset_simple_put_get_single},
  {"Simple PUT and GET of multiple values, filling to capacity",
    kvcacheset_simple_put_get_multiple},
  {"Simple DEL test", kvcacheset_del_simple},
  {"PUT with overwriting of keys", kvcacheset_put_overwrite},
  {"PUT with overfull cache, replacement policy when no ref bits set",
    kvcacheset_replacement_no_ref_bits},
  {"PUT with overfull cache, replacement policy when all ref bits set",
    kvcacheset_replacement_all_ref_bits},
  {"Clearing the cache set", kvcacheset_clear_all},
  NULL_TEST_INFO
};

suite_info_t kvcacheset_suite = {"KVCacheSet Tests", kvcacheset_test_init,
  NULL, kvcacheset_tests};
