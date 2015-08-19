#ifndef __TESTER__
#define __TESTER__

#define ASSERT(actual) if (!(actual)) return 0;
#define ASSERT_TRUE(actual) if (!(actual)) return 0;
#define ASSERT_FALSE(actual) if (actual) return 0;
#define ASSERT_PTR_NULL(actual) if ((actual) != NULL) return 0;
#define ASSERT_PTR_NOT_NULL(actual) if ((actual) == NULL) return 0;
#define ASSERT_EQUAL(actual, expect) if ((actual)!=(expect)) return 0;
#define ASSERT_NOT_EQUAL(actual, expect) if ((actual)==(expect)) return 0;
#define ASSERT_NOT_EQUAL(actual, expect) if ((actual)==(expect)) return 0;
#define ASSERT_STRING_EQUAL(actual, expect) if (strcmp(actual,expect)!=0) return 0;
#define ASSERT_STRING_NOT_EQUAL(actual, expect) if (strcmp(actual,expect)==0) return 0;

typedef int (*init_suite_func_t)(void);
typedef int (*clean_suite_func_t)(void);
typedef int (*test_func_t)(void);

#define NULL_TEST_INFO {NULL, NULL}
#define IS_NULL_TEST_INFO(tinfo) \
  ((tinfo.description==NULL) && (tinfo.func==NULL))

#define NULL_SUITE_INFO {NULL, NULL, NULL, NULL}
#define IS_NULL_SUITE_INFO(sinfo) \
  ((sinfo.description==NULL) && (sinfo.init==NULL) && (sinfo.clean==NULL) && \
   (sinfo.funcs==NULL))

#define KNRM  "\x1B[0m"
#define KRED  "\x1B[31m"
#define KGRN  "\x1B[32m"
#define KYEL  "\x1B[33m"
#define KBLU  "\x1B[34m"
#define KMAG  "\x1B[35m"
#define KCYN  "\x1B[36m"
#define KWHT  "\x1B[37m"

typedef struct {
  char *description;
  test_func_t func;
} test_info_t;

typedef struct {
  char *description;
  init_suite_func_t init;
  clean_suite_func_t clean;
  test_info_t *funcs;
} suite_info_t;

#endif
