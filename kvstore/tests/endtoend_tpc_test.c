#include <unistd.h>
#include <sys/socket.h>
#include <pthread.h>
#include <stdio.h>
#include "kvserver.h"
#include "tester.h"
#include "socket_server.h"

#define ENDTOEND_TPC_HOSTNAME "localhost"
#define ENDTOEND_TPC_SLAVE_PORT_1 8162
#define ENDTOEND_TPC_SLAVE_NAME_1 "endtoend_tpc_slave1"
#define ENDTOEND_TPC_SLAVE_PORT_2 8163
#define ENDTOEND_TPC_SLAVE_NAME_2 "endtoend_tpc_slave2"
#define ENDTOEND_TPC_MASTER_PORT 8164

server_t socket_slave1, socket_slave2, socket_master;
kvserver_t *slave1, *slave2;
tpcmaster_t *master;
pthread_mutex_t endtoend_tpc_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t endtoend_tpc_cond = PTHREAD_COND_INITIALIZER;
int requests_before_death, synch, completed, servers_running;

pthread_t master_thread, slave1_thread, slave2_thread;

kvhandle_t old_slave_handle;
void (*old_master_handle)(tpcmaster_t*, int, callback_t);
void* (*client_thread)(void*);
void endtoend_tpc_server_run_callback(void* aux);
void endtoend_tpc_handle_then_die(kvserver_t* server, int sockfd, void* extra);

int endtoend_tpc_test_init(void) {
  completed = 0;
  servers_running = 0;

  socket_slave1.master = 0;
  socket_slave1.max_threads = 1;
  slave1 = &socket_slave1.kvserver;

  socket_slave2.master = 0;
  socket_slave2.max_threads = 1;
  slave2 = &socket_slave2.kvserver;

  socket_master.master = 1;
  socket_master.max_threads = 2;
  master = &socket_master.tpcmaster;
  tpcmaster_init(master, 2, 2, 2, 2);

  return 0;
}

int endtoend_tpc_test_clean(void) {
  return kvserver_clean(slave1) + kvserver_clean(slave2);
}

void *endtoend_tpc_slave_runner(void *_slave_num) {
  int sockfd, slave_num = (intptr_t) _slave_num;
  server_t *socket_server;
  kvserver_t *slave;

  socket_server = (slave_num == 1) ? &socket_slave1 : &socket_slave2;
  slave = &socket_server->kvserver;
  if (slave_num == 1) {
    kvserver_init(slave, ENDTOEND_TPC_SLAVE_NAME_1, 2, 2, 1, ENDTOEND_TPC_HOSTNAME,
        ENDTOEND_TPC_SLAVE_PORT_1, 1);
    old_slave_handle = slave->handle;
    slave->handle = &endtoend_tpc_handle_then_die;
  } else {
    kvserver_init(slave, ENDTOEND_TPC_SLAVE_NAME_2, 2, 2, 1, ENDTOEND_TPC_HOSTNAME,
        ENDTOEND_TPC_SLAVE_PORT_2, 1);
  }
  /* Rebuild state from TPC Log in case the server is recovering from a (simulated) crash */
  kvserver_rebuild_state(slave);

  sockfd = connect_to(ENDTOEND_TPC_HOSTNAME, ENDTOEND_TPC_MASTER_PORT, 10);
  kvserver_register_master(slave, sockfd);
  close(sockfd);

  server_run(ENDTOEND_TPC_HOSTNAME, slave->port, socket_server, endtoend_tpc_server_run_callback);
  return NULL;
}

void *endtoend_tpc_master_runner(void *aux) {
  server_run(ENDTOEND_TPC_HOSTNAME, ENDTOEND_TPC_MASTER_PORT, &socket_master,
      endtoend_tpc_server_run_callback);
  return NULL;
}

/* Spins up all three servers, waits until the main client_thread has finished
 * running, stops all of the servers, then returns whether or not the test passed. */
int endtoend_tpc_start_servers_wait_completion(void) {
  int pass;
  pthread_create(&master_thread, NULL, &endtoend_tpc_master_runner, NULL);

  /* Wait for master to be running before starting up slaves so they can register properly */
  pthread_mutex_lock(&endtoend_tpc_lock);
  while (servers_running < 1)
    pthread_cond_wait(&endtoend_tpc_cond, &endtoend_tpc_lock);
  pthread_mutex_unlock(&endtoend_tpc_lock);

  pthread_create(&slave1_thread, NULL, &endtoend_tpc_slave_runner, (void *) 1);
  pthread_create(&slave2_thread, NULL, &endtoend_tpc_slave_runner, (void *) 2);

  pthread_mutex_lock(&endtoend_tpc_lock);
  while (completed == 0)
    pthread_cond_wait(&endtoend_tpc_cond, &endtoend_tpc_lock);
  pass = (synch == 1);
  pthread_mutex_unlock(&endtoend_tpc_lock);

  server_stop(&socket_slave1);
  server_stop(&socket_slave2);
  server_stop(&socket_master);
  return pass;
}

/* Sends a message to the TPCMaster and waits for a response. */
kvmessage_t *endtoend_tpc_send_and_receive(kvmessage_t *reqmsg) {
  kvmessage_t *respmsg;
  int sockfd;
  sockfd = connect_to(ENDTOEND_TPC_HOSTNAME, ENDTOEND_TPC_MASTER_PORT, 10);
  kvmessage_send(reqmsg, sockfd);

  respmsg = kvmessage_parse(sockfd);
  shutdown(sockfd, SHUT_RDWR);
  close(sockfd);
  return respmsg;
}

/* Replacement for slave1's handle function. Handles normally, except that
 * immediately after REQUESTS_BEFORE_DEATH requests have been handled,
 * it stops slave1 from listening to any further requests to simulate a crash. */
void endtoend_tpc_handle_then_die(kvserver_t *server, int sockfd, void *extra) {
  static int num_handles = 0;
  old_slave_handle(server, sockfd, extra);
  pthread_mutex_lock(&endtoend_tpc_lock);
  if ((requests_before_death != 0) && (++num_handles == requests_before_death))
    server_stop(&socket_slave1);
  pthread_mutex_unlock(&endtoend_tpc_lock);
}

/* Called when the master fails to connect to a slave, and once with a NULL
 * parameter between the two phases. */
void endtoend_tpc_master_failure_callback(void *_tpcslave) {
  tpcslave_t *tpcslave;
  static int slave1_fails = 0;
  static bool phase_two = false;
  if (!_tpcslave) {
    phase_two = true;
    slave1_fails = 0;
    return;
  }
  tpcslave = (tpcslave_t*) _tpcslave;
  if (tpcslave->port == slave1->port)
    slave1_fails++;
  if (phase_two && slave1_fails == 2) {
    /* Restart slave1 once the master has attempted and failed to contact it twice */
    pthread_create(&slave1_thread, NULL, &endtoend_tpc_slave_runner, (void *) 1);
  }
}

/* Mostly just a pass-through, but adds in a callback to the TPCMaster's handle
 * to be able to detect failures. */
void endtoend_tpc_handle_master(tpcmaster_t *master, int sockfd, callback_t callback) {
  old_master_handle(master, sockfd, endtoend_tpc_master_failure_callback);
}

/* Gets called once each time one of the servers is ready to run;
 * once all three are running, starts CLIENT_THREAD, which is the
 * main testing thread function. */
void endtoend_tpc_server_run_callback(void *aux) {
  pthread_t thread;
  pthread_mutex_lock(&endtoend_tpc_lock);
  servers_running++;
  if (servers_running == 1)
    pthread_cond_signal(&endtoend_tpc_cond);
  else if (servers_running == 3)
    pthread_create(&thread, NULL, client_thread, NULL);
  pthread_mutex_unlock(&endtoend_tpc_lock);
}

void *endtoend_tpc_test_client_thread_failures(void *aux) {
  kvmessage_t reqmsg, *respmsg;
  int pass = 1;

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = PUTREQ;
  reqmsg.key = "key1";
  reqmsg.value = "value1";
  respmsg = endtoend_tpc_send_and_receive(&reqmsg);
  if (respmsg->type != RESP || strcmp(respmsg->message, MSG_SUCCESS) != 0)
    pass = 0;
  kvmessage_free(respmsg);

  tpcmaster_clear_cache(master);

  /* During this request, one of the slaves will be dead */
  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = GETREQ;
  reqmsg.key = "key1";
  respmsg = endtoend_tpc_send_and_receive(&reqmsg);
  if ((respmsg->type != GETRESP) || (strcmp(respmsg->key, "key1") != 0)
      || (strcmp(respmsg->value, "value1") != 0))
    pass = 0;
  kvmessage_free(respmsg);

  /* Slave is dead; this should fail. Slave will restart to receive an ABORT. */
  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = PUTREQ;
  reqmsg.key = "key1";
  reqmsg.value = "newvalue";
  respmsg = endtoend_tpc_send_and_receive(&reqmsg);
  if (respmsg->type != RESP || strcmp(respmsg->message, MSG_SUCCESS) == 0)
    pass = 0;
  kvmessage_free(respmsg);

  /* Put shouldn't have gone through; should've had an ABORT above */
  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = GETREQ;
  reqmsg.key = "key1";
  respmsg = endtoend_tpc_send_and_receive(&reqmsg);
  if (respmsg->type != GETRESP || strcmp(respmsg->value, "value1") != 0)
    pass = 0;
  kvmessage_free(respmsg);

  /* Subsequent requests should function properly */
  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = DELREQ;
  reqmsg.key = "key1";
  respmsg = endtoend_tpc_send_and_receive(&reqmsg);
  if (respmsg->type != RESP)
    pass = 0;
  kvmessage_free(respmsg);

  tpcmaster_clear_cache(master);

  memset(&reqmsg, 0, sizeof(kvmessage_t));
  reqmsg.type = GETREQ;
  reqmsg.key = "key1";
  respmsg = endtoend_tpc_send_and_receive(&reqmsg);
  if (respmsg->type != RESP || strcmp(respmsg->message, ERRMSG_NO_KEY) != 0)
    pass = 0;
  kvmessage_free(respmsg);

  pthread_mutex_lock(&endtoend_tpc_lock);
  synch = pass;
  completed = 1;
  pthread_cond_signal(&endtoend_tpc_cond);
  pthread_mutex_unlock(&endtoend_tpc_lock);
  return 0;
}

int endtoend_tpc_failure_test(void) {
  requests_before_death = 2;
  client_thread = &endtoend_tpc_test_client_thread_failures;

  old_master_handle = master->handle;
  master->handle = &endtoend_tpc_handle_master;

  ASSERT_TRUE(endtoend_tpc_start_servers_wait_completion());
  return 1;
}


test_info_t endtoend_tpc_tests[] = {
  {"End to end test with tpc where one server dies and restarts",
    endtoend_tpc_failure_test},
  NULL_TEST_INFO
};

suite_info_t endtoend_tpc_suite = {"EndToEnd Tests using TPC",
  endtoend_tpc_test_init, endtoend_tpc_test_clean, endtoend_tpc_tests};
