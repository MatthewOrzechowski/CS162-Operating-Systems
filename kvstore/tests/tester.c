#define _XOPEN_SOURCE 500
#include <sys/wait.h>
#include <ftw.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include "tester.h"
#include "kvstore_test.h"
#include "kvcacheset_test.h"
#include "kvcache_test.h"
#include "kvserver_test.h"
#include "wq_test.h"
#include "socket_server_test.h"
#include "kvserver_tpc_test.h"
#include "tpclog_test.h"
#include "tpcmaster_test.h"
#include "kvserver_client_test.h"
#include "endtoend_test.h"
#include "endtoend_tpc_test.h"

#define TESTING_DIR "test_tmp_dir"

int tests_passed = 0, tests_failed = 0, suites_run = 0;

typedef enum {
  PASS,
  FAIL,
  SEGFAULT
} success_mode_t;

/* Test suite lookup table */
struct suite_desc {
  suite_info_t suite;
  char *name;
};

int lookup(const char suite[], struct suite_desc suite_table[],
           int num_suites) {
  int i;
  for (i = 0; i < num_suites; i++) {
    if (suite && (strcmp(suite_table[i].name, suite) == 0)) return i;
  }
  return -1;
}

int rm_all_helper(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf) {
  int rv;
  rv = remove(fpath);
  if (rv)
    perror(fpath);
  return rv;
}

int rm_all(char *path) {
  return nftw(path, rm_all_helper, 64, FTW_DEPTH | FTW_PHYS);
}

static success_mode_t safe_run(test_func_t func, init_suite_func_t init,
                     clean_suite_func_t clean) {
  pid_t c_pid = fork();
  int status, func_ret;
  if (c_pid < 0){
    printf("FORK FAILED\n");
    return 0;
  }
  else if (c_pid > 0) {
    waitpid(c_pid, &status, 0);
    rm_all(TESTING_DIR);
    if (WIFSIGNALED(status)==1 && WTERMSIG(status)==SIGSEGV)
      return SEGFAULT;
    return (WIFEXITED(status) && (WEXITSTATUS(status) == 0)) ? PASS : FAIL;
  } else {
    rm_all(TESTING_DIR);
    mkdir(TESTING_DIR, 0700);
    chdir(TESTING_DIR);
    if (init != NULL) init();
    func_ret = func();
    if (clean != NULL) clean();
    exit(func_ret == 0);
  }
}

static bool safe_run_suite(suite_info_t *suite) {
  int success, i = 0, failed = 0, passed = 0;
  const char* segfault_msg = " (segfaulted) ";
  printf("Starting Test Suite %s\n", suite->description);
  suites_run++;
  while (!IS_NULL_TEST_INFO(suite->funcs[i])) {
    if ((success = safe_run(suite->funcs[i].func, suite->init, suite->clean)) == FAIL
        || success == SEGFAULT) {
      printf("Test %s - %sFAILED%s%s\n", suite->funcs[i].description, KRED,
             (success == SEGFAULT) ? segfault_msg : "", KNRM);
      failed++;
      tests_failed++;
    } else {
      printf("Test %s  - %sPASSED%s\n", suite->funcs[i].description, KGRN,KNRM);
      passed++;
      tests_passed++;
    }
    i++;
  }
  printf("\nTest Suite %s: %d passed, %d failed\n\n", suite->description,
      passed, failed);
  return failed == 0;
}

int main(int argc, const char *argv[]) {
  int suite_num, i;
  suite_info_t *suites;
  char const *suite;

  struct suite_desc suite_table[] = {
    {kvstore_suite, "kvstore"},
    {kvcacheset_suite, "kvcacheset"},
    {kvcache_suite, "kvcache"},
    {kvserver_suite, "kvserver"},
    {wq_suite, "wq"},
    {socket_server_suite, "socket_server"},
    {kvserver_client_suite, "kvserver_client"},
    {kvserver_tpc_suite, "kvserver_tpc"},
    {tpclog_suite, "tpclog"},
    {tpcmaster_suite, "tpcmaster"},
    {endtoend_suite, "endtoend"},
    {endtoend_tpc_suite, "endtoend_tpc"}
  };
  int num_suites = sizeof(suite_table) / sizeof(struct suite_desc);

  suite_info_t single_suite[] = {NULL_SUITE_INFO, NULL_SUITE_INFO};

  suite_info_t checkpoint1_suites[] = {
    kvcacheset_suite,
    kvcache_suite,
    kvserver_suite,
    wq_suite,
    socket_server_suite,
    endtoend_suite,
    NULL_SUITE_INFO
  };

  suite_info_t all_suites[] = {
    kvcacheset_suite,
    kvcache_suite,
    kvserver_suite,
    wq_suite,
    socket_server_suite,
    endtoend_suite,
    kvserver_tpc_suite,
    tpcmaster_suite,
    endtoend_tpc_suite,
    NULL_SUITE_INFO
  };

  if (argc < 2 || strcmp("checkpoint2", (suite = argv[1])) == 0
      || strcmp("all", suite) == 0) {
    suites = all_suites;
  } else if (strcmp("checkpoint1", suite) == 0) {
    suites = checkpoint1_suites;
  } else {
    suite_num = lookup(suite, suite_table, num_suites);
    if (suite_num < 0) {
      printf("Error: Invalid suite name passed in. Valid suite names are:"
             "\ncheckpoint1\ncheckpoint2\nall\n\n");
      for (i = 0; i < num_suites; i++)
        printf("%s\n", suite_table[i].name);
      exit(-1);
    }
    single_suite[0] = suite_table[suite_num].suite;
    suites = single_suite;
  }

  i = 0;

  while (!IS_NULL_SUITE_INFO(suites[i])) {
    safe_run_suite(&suites[i++]);
  }

  printf("A total of %d suites were run\n%d tests passed, %d tests failed\n",
      suites_run, tests_passed, tests_failed);
}
