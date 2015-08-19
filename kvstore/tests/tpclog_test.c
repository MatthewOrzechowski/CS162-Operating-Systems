#include <dirent.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "tpclog.h"
#include "tester.h"

#define TPCLOG_DIRNAME "tpclog-test"

tpclog_t testlog;

/* Deletes all current entries in the store and the log and removes the entire
 * directory. */
int tpclog_clean(void) {
  struct dirent *dent;
  char filename[MAX_FILENAME];
  DIR *tpclogdir = opendir(TPCLOG_DIRNAME);
  if (tpclogdir == NULL)
    return 0;
  while ((dent = readdir(tpclogdir)) != NULL) {
    sprintf(filename, "%s/%s", TPCLOG_DIRNAME, dent->d_name);
    remove(filename);
  }
  closedir(tpclogdir);
  remove(TPCLOG_DIRNAME);
  return 0;
}

int tpclog_test_init(void) {
  tpclog_init(&testlog, TPCLOG_DIRNAME);
  return 0;
}

int tpclog_log_load(void) {
  int ret;
  char filename[MAX_FILENAME];
  logentry_t *entry;
  ret = tpclog_log(&testlog, PUTREQ, "MYKEY", "MYVALUE");
  ASSERT_EQUAL(ret, 0);
  strcpy(filename, TPCLOG_DIRNAME);
  strcat(filename, "/0");
  strcat(filename, TPCLOG_FILETYPE);
  ret = tpclog_load_entry(&entry, filename);
  ASSERT_EQUAL(ret, 0);
  ASSERT_EQUAL(entry->type, PUTREQ);
  ASSERT_EQUAL(entry->length, 14);
  ASSERT_STRING_EQUAL(entry->data, "MYKEY");
  ASSERT_STRING_EQUAL(entry->data + 6, "MYVALUE");
  free(entry);
  return 1;
}

int tpclog_log_load_multiple(void) {
  int ret;
  char filename[MAX_FILENAME];
  logentry_t *entry;
  ret = tpclog_log(&testlog, PUTREQ, "MYKEY", "MYVALUE");
  ret += tpclog_log(&testlog, DELREQ, "MYKEY", NULL);
  ret += tpclog_log(&testlog, COMMIT, NULL, NULL);
  ret += tpclog_log(&testlog, ABORT, NULL, NULL);
  ASSERT_EQUAL(ret, 0);

  strcpy(filename, TPCLOG_DIRNAME);
  strcat(filename, "/0");
  strcat(filename, TPCLOG_FILETYPE);
  ret = tpclog_load_entry(&entry, filename);
  ASSERT_EQUAL(ret, 0);
  ASSERT_EQUAL(entry->type, PUTREQ);
  ASSERT_EQUAL(entry->length, 14);
  ASSERT_STRING_EQUAL(entry->data, "MYKEY");
  ASSERT_STRING_EQUAL(entry->data + 6, "MYVALUE");
  free(entry);

  strcpy(filename, TPCLOG_DIRNAME);
  strcat(filename, "/1");
  strcat(filename, TPCLOG_FILETYPE);
  ret = tpclog_load_entry(&entry, filename);
  ASSERT_EQUAL(ret, 0);
  ASSERT_EQUAL(entry->type, DELREQ);
  ASSERT_EQUAL(entry->length, 6);
  ASSERT_STRING_EQUAL(entry->data, "MYKEY");
  free(entry);

  strcpy(filename, TPCLOG_DIRNAME);
  strcat(filename, "/2");
  strcat(filename, TPCLOG_FILETYPE);
  ret = tpclog_load_entry(&entry, filename);
  ASSERT_EQUAL(ret, 0);
  ASSERT_EQUAL(entry->type, COMMIT);
  ASSERT_EQUAL(entry->length, 0);
  free(entry);

  strcpy(filename, TPCLOG_DIRNAME);
  strcat(filename, "/3");
  strcat(filename, TPCLOG_FILETYPE);
  ret = tpclog_load_entry(&entry, filename);
  ASSERT_EQUAL(ret, 0);
  ASSERT_EQUAL(entry->type, ABORT);
  ASSERT_EQUAL(entry->length, 0);
  free(entry);
  return 1;
}

int tpclog_test_clear_log(void) {
  int ret;
  char filename[MAX_FILENAME];
  logentry_t *entry;
  ret = tpclog_log(&testlog, ABORT, NULL, NULL);
  ret += tpclog_log(&testlog, PUTREQ, "MYKEY", "MYVALUE");
  ret += tpclog_log(&testlog, DELREQ, "MYKEY", NULL);
  ret += tpclog_log(&testlog, COMMIT, NULL, NULL);
  ret += tpclog_log(&testlog, ABORT, NULL, NULL);
  ret += tpclog_log(&testlog, ABORT, NULL, NULL);
  ret += tpclog_clear_log(&testlog);
  ret += tpclog_log(&testlog, PUTREQ, "NEWKEY", "NEWVALUE");
  ASSERT_EQUAL(ret, 0);

  /* After clearing the log, our new log entry should be the first one. */
  strcpy(filename, TPCLOG_DIRNAME);
  strcat(filename, "/0");
  strcat(filename, TPCLOG_FILETYPE);
  ret = tpclog_load_entry(&entry, filename);
  ASSERT_EQUAL(ret, 0);
  ASSERT_EQUAL(entry->type, PUTREQ);
  ASSERT_EQUAL(entry->length, 16);
  ASSERT_STRING_EQUAL(entry->data, "NEWKEY");
  ASSERT_STRING_EQUAL(entry->data + 7, "NEWVALUE");
  free(entry);
  return 1;
}

int tpclog_iterate_entries(void) {
  int ret;
  logentry_t *entry;
  ret = tpclog_log(&testlog, PUTREQ, "MYKEY", "MYVALUE");
  ret += tpclog_log(&testlog, DELREQ, "MYKEY", NULL);
  ret += tpclog_log(&testlog, COMMIT, NULL, NULL);
  ret += tpclog_log(&testlog, ABORT, NULL, NULL);
  ret += tpclog_log(&testlog, COMMIT, NULL, NULL);
  ret += tpclog_log(&testlog, DELREQ, "NEWKEY", NULL);
  ASSERT_EQUAL(ret, 0);

  tpclog_iterate_begin(&testlog);

  ASSERT_TRUE(tpclog_iterate_has_next(&testlog));
  entry = tpclog_iterate_next(&testlog);
  ASSERT_EQUAL(entry->type, PUTREQ);
  ASSERT_EQUAL(entry->length, 14);
  ASSERT_STRING_EQUAL(entry->data, "MYKEY");
  ASSERT_STRING_EQUAL(entry->data + 6, "MYVALUE");
  free(entry);

  ASSERT_TRUE(tpclog_iterate_has_next(&testlog));
  entry = tpclog_iterate_next(&testlog);
  ASSERT_EQUAL(entry->type, DELREQ);
  ASSERT_EQUAL(entry->length, 6);
  ASSERT_STRING_EQUAL(entry->data, "MYKEY");
  free(entry);

  ASSERT_TRUE(tpclog_iterate_has_next(&testlog));
  entry = tpclog_iterate_next(&testlog);
  ASSERT_EQUAL(entry->type, COMMIT);
  ASSERT_EQUAL(entry->length, 0);
  free(entry);

  ASSERT_TRUE(tpclog_iterate_has_next(&testlog));
  entry = tpclog_iterate_next(&testlog);
  ASSERT_EQUAL(entry->type, ABORT);
  ASSERT_EQUAL(entry->length, 0);
  free(entry);

  ASSERT_TRUE(tpclog_iterate_has_next(&testlog));
  entry = tpclog_iterate_next(&testlog);
  ASSERT_EQUAL(entry->type, COMMIT);
  ASSERT_EQUAL(entry->length, 0);
  free(entry);

  ASSERT_TRUE(tpclog_iterate_has_next(&testlog));
  entry = tpclog_iterate_next(&testlog);
  ASSERT_EQUAL(entry->type, DELREQ);
  ASSERT_EQUAL(entry->length, 7);
  ASSERT_STRING_EQUAL(entry->data, "NEWKEY");
  free(entry);

  ASSERT_FALSE(tpclog_iterate_has_next(&testlog));
  ASSERT_PTR_NULL(tpclog_iterate_next(&testlog));
  return 1;
}

test_info_t tpclog_tests[] = {
  {"Simple test of logging an entry and loading it back", tpclog_log_load},
  {"Simple test of logging multiple entries and loading them back",
    tpclog_log_load_multiple},
  {"Simple test of clearing out the log", tpclog_test_clear_log},
  {"Iterate through entries", tpclog_iterate_entries},
  NULL_TEST_INFO
};

suite_info_t tpclog_suite = {"TPCLog Tests", tpclog_test_init, tpclog_clean,
  tpclog_tests};
