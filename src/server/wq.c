#include <stdlib.h>
#include "wq.h"
#include "kvconstants.h"
#include "utlist.h"

/* Initializes a work queue WQ. Sets up any necessary synchronization constructs. */
void wq_init(wq_t *wq) {
  wq->head = NULL;
  pthread_mutex_init(&wq->lock, 0); // init the lock
  pthread_cond_init(&wq->lock, 0); // init the lock
}

/* Remove an item from the WQ. Currently, this immediately attempts
 * to remove the item at the head of the list, and will fail if there are
 * no items in the list.
 *
 * It is your task to make it so that this function will wait until the queue
 * contains at least one item, then remove that item from the list and
 * return it. */
void *wq_pop(wq_t *wq) {
  pthread_mutex_lock(&wq->lock);
  while (wq->head == NULL) {
    // wait for a job.  Once cond_wait returns, we own the lock, and there is a job in the queue.
    pthread_cond_wait(&wq->cond_var, &wq->lock);
  }
  void *job;
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
  pthread_mutex_lock(&wq->lock);
  wq_item_t *wq_item = calloc(1, sizeof(wq_item_t));
  wq_item->item = item;
  DL_APPEND(wq->head, wq_item);
  pthread_cond_signal(&wq->cond_var); //signal pop
  pthread_mutex_unlock(&wq->lock);
}

/* Checks if the WQ contains no elements.  Returns 1 if the WQ is empty, 0 otherwise */
int wq_is_empty(wq_t *wq) {
  pthread_mutex_lock(&wq->lock);
  int ret = (wq->head == NULL);
  pthread_mutex_unlock(&wq->lock);
  return ret;
}

/* Emptys the WQ.  This is used to prevent memory leaks. */
int wq_clear(wq_t *wq) {
  pthread_mutex_lock(&wq->lock);
  while (wq->head != NULL) {
    void *job;
    job = wq->head->item;
    DL_DELETE(wq->head,wq->head);
  }
  pthread_mutex_unlock(&wq->lock);
  return 1;
}


