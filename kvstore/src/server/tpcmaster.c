#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netdb.h>
#include "kvconstants.h"
#include "kvmessage.h"
#include "socket_server.h"
#include "time.h"
#include "tpcmaster.h"

static tpc_state_t state = TPC_READY;

/* Initializes a tpcmaster. Will return 0 if successful, or a negative error
 * code if not. SLAVE_CAPACITY indicates the maximum number of slaves that
 * the master will support. REDUNDANCY is the number of replicas (slaves) that
 * each key will be stored in. The master's cache will have NUM_SETS cache sets,
 * each with ELEM_PER_SET elements. */
int tpcmaster_init(tpcmaster_t *master, unsigned int slave_capacity,
    unsigned int redundancy, unsigned int num_sets, unsigned int elem_per_set) {
  int ret;
  ret = kvcache_init(&master->cache, num_sets, elem_per_set);
  if (ret < 0) return ret;
  ret = pthread_rwlock_init(&master->slave_lock, NULL);
  if (ret < 0) return ret;
  master->slave_count = 0;
  master->slave_capacity = slave_capacity;
  if (redundancy > slave_capacity) {
    master->redundancy = slave_capacity;
  } else {
    master->redundancy = redundancy;
  }
  master->slaves_head = NULL;
  master->handle = tpcmaster_handle;
  return 0;
}

/* Converts Strings to 64-bit longs. Borrowed from http://goo.gl/le1o0W,
 * adapted from the Java builtin String.hashcode().
 * DO NOT CHANGE THIS FUNCTION. */
int64_t hash_64_bit(char *s) {
  int64_t h = 1125899906842597LL;
  int i;
  for (i = 0; s[i] != 0; i++) {
    h = (31 * h) + s[i];
  }
  return h;
}

/* Handles an incoming kvmessage REQMSG, and populates the appropriate fields
 * of RESPMSG as a response. RESPMSG and REQMSG both must point to valid
 * kvmessage_t structs. Assigns an ID to the slave by hashing a string in the
 * format PORT:HOSTNAME, then tries to add its info to the MASTER's list of
 * slaves. If the slave is already in the list, do nothing (success).
 * There can never be more slaves than the MASTER's slave_capacity. RESPMSG
 * will have MSG_SUCCESS if registration succeeds, or an error otherwise.
 *
 * Checkpoint 2 only. */
void tpcmaster_register(tpcmaster_t *master, kvmessage_t *reqmsg,
    kvmessage_t *respmsg) {
  if (master->slave_count >= master->slave_capacity) {
    respmsg->message = ERRMSG_GENERIC_ERROR;
    return;
  }
  
  //add stats
  tpcslave_t * new = malloc(sizeof(tpcslave_t));
  new->host = calloc(1, strlen(reqmsg->key) + 1);
  memcpy(new->host, reqmsg->key, strlen(reqmsg->key));
  new->port = (unsigned int) atoi(reqmsg->value);
  char temp[512];
  strcpy(temp, reqmsg->value);
  strcat(temp, ":");
  strcat(temp, new->host);
  new->id = hash_64_bit(temp);
  
  //put in list
  pthread_rwlock_wrlock(&master->slave_lock);
  if (!master->slaves_head){
    master->slaves_head = new;
    master->slave_count++;
    respmsg->message = MSG_SUCCESS;
    pthread_rwlock_unlock(&master->slave_lock);
    return;
  }
  tpcslave_t * curr = master->slaves_head;
  while (curr->next){
    if (curr->next->id >= new->id) break;
    curr = curr->next;
  }
  if (!curr->next){
    if (curr->id == new->id){
      respmsg->message = MSG_SUCCESS;
      pthread_rwlock_unlock(&master->slave_lock);
      return;
    } else {
      new->prev = curr;
      curr->next = new;
      master->slave_count++;
      respmsg->message = MSG_SUCCESS;
      pthread_rwlock_unlock(&master->slave_lock);
      return;
    }
  }
  if (curr->next->id == new->id){
    respmsg->message = MSG_SUCCESS;
    pthread_rwlock_unlock(&master->slave_lock);
    return;
  } else {
    new->prev = curr;
    new->next = curr->next;
    curr->next = new;
    new->next->prev = new;
    master->slave_count++;
    respmsg->message = MSG_SUCCESS;
    pthread_rwlock_unlock(&master->slave_lock);
    return;
  }
}

/* Hashes KEY and finds the first slave that should contain it.
 * It should return the first slave whose ID is greater than the
 * KEY's hash, and the one with lowest ID if none matches the
 * requirement.
 *
 * Checkpoint 2 only. */
tpcslave_t *tpcmaster_get_primary(tpcmaster_t *master, char *key) {
  int64_t hash = hash_64_bit(key);
  tpcslave_t * curr = master->slaves_head;
  pthread_rwlock_rdlock(&master->slave_lock);
  do{
    if (hash <= curr->id) {
      pthread_rwlock_unlock(&master->slave_lock);
      return curr;
    }
    curr = curr->next;
  } while (curr->next);
  pthread_rwlock_unlock(&master->slave_lock);
  return master->slaves_head;
}

/* Returns the slave whose ID comes after PREDECESSOR's, sorted
 * in increasing order.
 *
 * Checkpoint 2 only. */
tpcslave_t *tpcmaster_get_successor(tpcmaster_t *master,
    tpcslave_t *predecessor) {
  pthread_rwlock_rdlock(&master->slave_lock);
  if (!predecessor->next) {
    pthread_rwlock_unlock(&master->slave_lock);
    return master->slaves_head;
  }
  pthread_rwlock_unlock(&master->slave_lock);
  return predecessor->next;
}

/* Handles an incoming GET request REQMSG, and populates the appropriate fields
 * of RESPMSG as a response. RESPMSG and REQMSG both must point to valid
 * kvmessage_t structs.
 *
 * Checkpoint 2 only. */
void tpcmaster_handle_get(tpcmaster_t *master, kvmessage_t *reqmsg,
    kvmessage_t *respmsg) {
  //check cache (maybe return)
  int check = kvcache_get(&master->cache, reqmsg->key, &respmsg->value);
  if (!check){
    respmsg->type = GETRESP;
    respmsg->key = malloc(256);
    strcpy(respmsg->key, reqmsg->key);
    return;
  }
  //check if right number of slaves
  if (master->slave_count != master->slave_capacity) {
    respmsg->message = ERRMSG_GENERIC_ERROR;
    return;
  }
  //go to slaves
  kvmessage_t temp_reqmsg, *temp_respmsg;
  memcpy(&temp_reqmsg, reqmsg, sizeof(kvmessage_t));
  int i;
  int success = 0;
  tpcslave_t * curr = tpcmaster_get_primary(master, reqmsg->key);
  for (i = 0; i < master->redundancy; i++){
    int fd = connect_to(curr->host, curr->port, 100);
    if (fd != -1) {
      kvmessage_send(&temp_reqmsg, fd);
      temp_respmsg = kvmessage_parse(fd);
      if (temp_respmsg && temp_respmsg->type == GETRESP){
        success = 1;
        break;
      }
    }
    curr = tpcmaster_get_successor(master, curr);
  }
  if (success){
    memcpy(respmsg, temp_respmsg, sizeof(kvmessage_t));
    //update cache
    kvcache_put(&master->cache, respmsg->key, respmsg->value);
  }
  else{
    respmsg->type = RESP;
    respmsg->message = ERRMSG_NO_KEY;
  }
}

/* Handles an incoming TPC request REQMSG, and populates the appropriate fields
 * of RESPMSG as a response. RESPMSG and REQMSG both must point to valid
 * kvmessage_t structs. Implements the TPC algorithm, polling all the slaves
 * for a vote first and sending a COMMIT or ABORT message in the second phase.
 * Must wait for an ACK from every slave after sending the second phase messages. 
 * 
 * The CALLBACK field is used for testing purposes. You MUST include the following
 * calls to the CALLBACK function whenever CALLBACK is not null, or you will fail
 * some of the tests:
 * - During both phases of contacting slaves, whenever a slave cannot be reached (i.e. you
 *   attempt to connect and receive a socket fd of -1), call CALLBACK(slave), where
 *   slave is a pointer to the tpcslave you are attempting to contact.
 * - Between the two phases, call CALLBACK(NULL) to indicate that you are transitioning
 *   between the two phases.  
 * 
 * Checkpoint 2 only. */
void tpcmaster_handle_tpc(tpcmaster_t *master, kvmessage_t *reqmsg,
    kvmessage_t *respmsg, callback_t callback) {
  if (master->slave_count != master->slave_capacity) {
    respmsg->message = ERRMSG_GENERIC_ERROR;
    return;
  }
  if (state != TPC_READY){
    respmsg->message = ERRMSG_INVALID_REQUEST;
    return;
  }
  state = TPC_INIT;
  //have slaves vote
  tpcslave_t * curr = tpcmaster_get_primary(master, reqmsg->key);
  kvmessage_t temp_reqmsg, *temp_respmsg;
  memcpy(&temp_reqmsg, reqmsg, sizeof(kvmessage_t));
  int i, fd;
  int commit = 1;
  for (i = 0; i < master->redundancy; i++){
    fd = connect_to(curr->host, curr->port, 100);
    if (fd == -1 && callback) callback(curr);
    kvmessage_send(&temp_reqmsg, fd);
    temp_respmsg = kvmessage_parse(fd);
    if (!temp_respmsg || temp_respmsg->type == VOTE_ABORT) commit = 0;
    curr = tpcmaster_get_successor(master, curr);
  }
  //phase change
  if (callback) callback(NULL);
  if (commit) {
    state = TPC_COMMIT;
    temp_reqmsg.type = COMMIT;
  } else {
    state = TPC_ABORT;
    temp_reqmsg.type = ABORT;
  }
  //send out command
  for (i = 0; i < master->redundancy; i++){
    temp_respmsg = NULL;
    while (!temp_respmsg){
      fd = connect_to(curr->host, curr->port, 100);
      if (fd == -1) callback(curr);
      kvmessage_send(&temp_reqmsg, fd);
      temp_respmsg = kvmessage_parse(fd);
    }
    curr = tpcmaster_get_successor(master, curr);
  }
  //populate respmsg
  if (state == TPC_COMMIT) {
    respmsg->message = MSG_SUCCESS;
    if (reqmsg->type == DELREQ) kvcache_del(&master->cache, reqmsg->key);
  }
  else respmsg->message = ERRMSG_GENERIC_ERROR;
  state = TPC_READY;
}

/* Handles an incoming kvmessage REQMSG, and populates the appropriate fields
 * of RESPMSG as a response. RESPMSG and REQMSG both must point to valid
 * kvmessage_t structs. Provides information about the slaves that are
 * currently alive.
 *
 * Checkpoint 2 only. */
void tpcmaster_info(tpcmaster_t *master, kvmessage_t *reqmsg,
    kvmessage_t *respmsg) {
  time_t clk = time(NULL);
  respmsg->message = malloc(sizeof("TIMESTAMP: "));
  strcpy(respmsg->message, "TIMESTAMP: ");
  respmsg->message = realloc(respmsg->message, strlen(respmsg->message) + strlen(ctime(&clk)) + strlen("Slaves:\n"));
  strcat(respmsg->message, ctime(&clk));
  strcat(respmsg->message, "Slaves:");

  tpcslave_t * curr = master->slaves_head;
  int i;
  for (i = 0; i < master->slave_count; i++){
    int fd = connect_to(curr->host, curr->port, 2);
    if (fd != -1) {
      respmsg->message = realloc(respmsg->message, strlen(respmsg->message) + strlen(curr->host)+8);
      strcat(respmsg->message, "\n{");
      strcat(respmsg->message, curr->host);
      strcat(respmsg->message, ", ");
      char x[6];
      sprintf(x, "%d", curr->port);
      strcat(respmsg->message, x);
      strcat(respmsg->message, "}");
    }
    curr = curr->next;
  }
}

/* Generic entrypoint for this MASTER. Takes in a socket on SOCKFD, which
 * should already be connected to an incoming request. Processes the request
 * and sends back a response message.  This should call out to the appropriate
 * internal handler. */
void tpcmaster_handle(tpcmaster_t *master, int sockfd, callback_t callback) {
  kvmessage_t *reqmsg, respmsg;
  reqmsg = kvmessage_parse(sockfd);
  memset(&respmsg, 0, sizeof(kvmessage_t));
  respmsg.type = RESP;
  if (reqmsg->key != NULL) {
    respmsg.key = calloc(1, strlen(reqmsg->key));
    strcpy(respmsg.key, reqmsg->key);
  }
  if (reqmsg->type == INFO) {
    tpcmaster_info(master, reqmsg, &respmsg);
  } else if (reqmsg == NULL || reqmsg->key == NULL) {
    respmsg.message = ERRMSG_INVALID_REQUEST;
  } else if (reqmsg->type == REGISTER) {
    tpcmaster_register(master, reqmsg, &respmsg);
  } else if (reqmsg->type == GETREQ) {
    tpcmaster_handle_get(master, reqmsg, &respmsg);
  } else {
    tpcmaster_handle_tpc(master, reqmsg, &respmsg, callback);
  }
  kvmessage_send(&respmsg, sockfd);
  kvmessage_free(reqmsg);
  if (respmsg.key != NULL)
    free(respmsg.key);
}

/* Completely clears this TPCMaster's cache. For testing purposes. */
void tpcmaster_clear_cache(tpcmaster_t *tpcmaster) {
  kvcache_clear(&tpcmaster->cache);
}
