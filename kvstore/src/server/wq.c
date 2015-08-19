#include <stdlib.h>
#include "wq.h"
#include "kvconstants.h"
#include "utlist.h"

/* Initializes a work queue WQ. Sets up any necessary synchronization constructs. */
void wq_init(wq_t *wq) {
  wq->head = NULL;
  pthread_mutex_init(&wq->lock, NULL);
  pthread_cond_init(&wq->empty, NULL);  
}

/* Remove an item from the WQ. Currently, this immediately attempts
 * to remove the item at the head of the list, and will fail if there are
 * no items in the list.
 *
 * It is your task to make it so that this function will wait until the queue
 * contains at least one item, then remove that item from the list and
 * return it. */
void *wq_pop(wq_t *wq) {
  pthread_mutex_lock(&wq->lock); //tries to grab 'writer' lock              
  void *job;
  while (wq->head == NULL) {
    pthread_cond_wait(&wq->empty, &wq->lock);
  }                
  job = wq->head->item;
  DL_DELETE(wq->head,wq->head);
  pthread_mutex_unlock(&wq->lock);
  return job;
}

/* Add ITEM to WQ. Currently, this just adds ITEM to the list.
 *
 * It is your task to perform any necessary operations to properly
 * perform synchronization. */
void wq_push(wq_t *wq, void *item) {
  pthread_mutex_lock(&wq->lock); //tries to grab lock      
  wq_item_t *wq_item = calloc(1, sizeof(wq_item_t));
  wq_item->item = item;
  DL_APPEND(wq->head, wq_item);
  pthread_cond_signal(&wq->empty);
  pthread_mutex_unlock(&wq->lock);
}