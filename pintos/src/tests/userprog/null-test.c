/* Tests the null syscall */

#include "tests/lib.h"
#include "tests/main.h"
#include <stdio.h>

void
test_main (void) 
{
  int i = null(5);
  if (i != 6) {
    fail("Null syscall failed because i was not 6 as expected\n");
  }
  i = null(10);
  if (i != 11) {
    fail("Null syscall failed because i was not 11 as expected\n");
  }
  printf("null\n");
}
