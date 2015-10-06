#include <stdbool.h>
#include <stdio.h>
#include <errno.h>
#include <pthread.h>
#include "kvconstants.h"
#include "kvcache.h"
#include "kvstore.h"
#include "kvmessage.h"
#include "kvserver.h"
#include "tpclog.h"
#include "socket_server.h"
#include <stdlib.h>

/* Initializes a kvserver. Will return 0 if successful, or a negative error
 * code if not. DIRNAME is the directory which should be used to store entries
 * for this server.  The server's cache will have NUM_SETS cache sets, each
 * with ELEM_PER_SET elements.  HOSTNAME and PORT indicate where SERVER will be
 * made available for requests.  USE_TPC indicates whether this server should
 * use TPC logic (for PUTs and DELs) or not. */
int kvserver_init(kvserver_t *server, char *dirname, unsigned int num_sets,
    unsigned int elem_per_set, unsigned int max_threads, const char *hostname,
    int port, bool use_tpc) {
  int ret;
  ret = kvcache_init(&server->cache, num_sets, elem_per_set);
  if (ret < 0) return ret;
  ret = kvstore_init(&server->store, dirname);
  if (ret < 0) return ret;
  if (use_tpc) {
      ret = tpclog_init(&server->log, dirname);
      if (ret < 0) return ret;
  }
  server->hostname = malloc(strlen(hostname) + 1);
  if (server->hostname == NULL)
    return ENOMEM;
  strcpy(server->hostname, hostname);
  server->port = port;
  server->use_tpc = use_tpc;
  server->max_threads = max_threads;
  server->handle = kvserver_handle;
  server->pending_request = NULL;
  pthread_mutex_init(&server->pending_request_lock, NULL);
  return 0;
}

/* Sends a message to register SERVER with a TPCMaster over a socket located at
 * SOCKFD which has previously been connected. Does not close the socket when
 * done. Returns -1 if an error was encountered.
 *
 * Checkpoint 2 only. */
int kvserver_register_master(kvserver_t *server, int sockfd) {
  kvmessage_t *regmsg = (kvmessage_t *) calloc(1, sizeof(kvmessage_t));
  if (!regmsg) {
    return -1; 
  }
  regmsg->message = NULL;
  regmsg->type = REGISTER;
  regmsg->key = server->hostname;
  char *buf = (char*) malloc(128);
  sprintf(buf, "%d", server->port);
  regmsg->value = buf;
  kvmessage_send(regmsg, sockfd);
  free(buf);
  free(regmsg);
  /* After sending a register request to the master, we must wait for
   * a response, to check if registration was successful. */
  kvmessage_t *register_response = kvmessage_parse(sockfd);

  if (register_response->message != MSG_SUCCESS) {
    free(register_response);
    return -1;
  }
  free(register_response);
  return 0;
}

/* Attempts to get KEY from SERVER. Returns 0 if successful, else a negative
 * error code.  If successful, VALUE will point to a string which should later
 * be free()d.  If the KEY is in cache, take the value from there. Otherwise,
 * go to the store and update the value in the cache. */
int kvserver_get(kvserver_t *server, char *key, char **value) {
  pthread_rwlock_t *lock = kvcache_getlock(&server->cache, key);
  if (!lock) {
    return ERRKEYLEN;
  } 
  pthread_rwlock_rdlock(lock);
  int err = kvcache_get(&server->cache, key, value);
  pthread_rwlock_unlock(lock);

  if (err == 0) { 
    // key is in the cache   
    return 0;
  }
  // key is not in the cache (or kvcache_get errored), try reading from disk  
  err = kvstore_get(&server->store, key, value);
  if (err == 0) {
    pthread_rwlock_wrlock(lock);
    kvcache_put(&server->cache, key, *value);
    pthread_rwlock_unlock(lock);
    return 0;
  }
  return err;
}

/* Checks if the given KEY, VALUE pair can be inserted into this server's
 * store. Returns 0 if it can, else a negative error code. */
int kvserver_put_check(kvserver_t *server, char *key, char *value) {
  return kvstore_put_check(&server->store, key, value);
}

/* Inserts the given KEY, VALUE pair into this server's store and cache. Access
 * to the cache should be concurrent if the keys are in different cache sets.
 * Returns 0 if successful, else a negative error code. */
int kvserver_put(kvserver_t *server, char *key, char *value) {
  pthread_rwlock_t *lock = kvcache_getlock(&server->cache, key);
  if (!lock) {
    return ERRKEYLEN;
  } 
  int err = kvstore_put(&server->store, key, value);
  if (err) {
    return err;
  }

  pthread_rwlock_wrlock(lock);
  err = kvcache_put(&server->cache, key, value);
  pthread_rwlock_unlock(lock);
  if (err) {
    /* Put succeeded in the store, but failed in the cache, so remove it from the store */
    kvstore_del(&server->store, key);
    return err;
  }
  return 0;
}

/* Checks if the given KEY can be deleted from this server's store.
 * Returns 0 if it can, else a negative error code. */
int kvserver_del_check(kvserver_t *server, char *key) {
  return kvstore_del_check(&server->store, key);
}

/* Removes the given KEY from this server's store and cache. Access to the
 * cache should be concurrent if the keys are in different cache sets. Returns
 * 0 if successful, else a negative error code. */
int kvserver_del(kvserver_t *server, char *key) {
  pthread_rwlock_t *lock = kvcache_getlock(&server->cache, key);
  if (!lock) {
    return ERRKEYLEN;
  } 

  int err = kvstore_del(&server->store, key);
  if (err) {
    return err;
  }

  pthread_rwlock_wrlock(lock);
  err = kvcache_del(&server->cache, key);
  pthread_rwlock_unlock(lock);

  if (err && err!=ERRNOKEY) {
    /* We don't care about the case where the entry is not in the cache, but is in the store, 
    as this is valid behavior, and the function should return success (0).  */
    return err;
  }
  return 0;
}

/* Returns an info string about SERVER including its hostname and port. */
char *kvserver_get_info_message(kvserver_t *server) {
  char info[1024], buf[256];
  time_t ltime = time(NULL);
  strcpy(info, asctime(localtime(&ltime)));
  sprintf(buf, "{%s, %d}", server->hostname, server->port);
  strcat(info, buf);
  char *msg = malloc(strlen(info));
  strcpy(msg, info);
  return msg;
}

/* Handles an incoming kvmessage REQMSG, and populates the appropriate fields
 * of RESPMSG as a response. RESPMSG and REQMSG both must point to valid
 * kvmessage_t structs. Assumes that the request should be handled as a TPC
 * message. This should also log enough information in the server's TPC log to
 * be able to recreate the current state of the server upon recovering from
 * failure.  See the spec for details on logic and error messages.
 *
 * Checkpoint 2 only. */
void kvserver_handle_tpc(kvserver_t *server, kvmessage_t *reqmsg,
    kvmessage_t *respmsg) {
  respmsg->type = RESP;

  int err;
  switch(reqmsg->type) {
    case GETREQ:
      err = kvserver_get(server, reqmsg->key, &respmsg->value); 
      if (!err) {
        respmsg->key = reqmsg->key;
        respmsg->type = GETRESP;
      } else {
        respmsg->message = GETMSG(err);
      }
      return;

    case PUTREQ:
    case DELREQ:
      pthread_mutex_lock(&server->pending_request_lock);
      if (server->pending_request != NULL) {
        /* This PUTREQ or DELREQ is a duplicate (or we received another request before 
         * we committed an old request.  Don't log, send an error message response. */
        respmsg->message = ERRMSG_INVALID_REQUEST;
        pthread_mutex_unlock(&server->pending_request_lock);
        return;
      }
      /* Log the initial request, and store the request in
       * server->pending_request. 
       * Also we clear the log right before logging this request, as we are
       * guaranteed that the previous transaction is fully committed to the log 
       * by this point.  */
      tpclog_clear_log(&server->log);
      tpclog_log(&server->log, reqmsg->type, reqmsg->key, reqmsg->value);
      int vote_abort = 1;
      /* Check if we are able to satisfy the request */
      if (reqmsg->type == PUTREQ) {
        vote_abort = kvstore_put_check(&server->store, reqmsg->key, reqmsg->value);
      } else if (reqmsg->type == DELREQ) {
        vote_abort = kvstore_del_check(&server->store, reqmsg->key);
      }
      if (vote_abort) {
        respmsg->type = VOTE_ABORT;
        respmsg->message = GETMSG(vote_abort);
        // Slave should be ready for another request after sending vote abort, since it knows that this transaction will be aborted
        tpclog_log(&server->log, ABORT, NULL, NULL);
      } else {
        respmsg->type = VOTE_COMMIT;
        server->pending_request = (kvmessage_t*) malloc(sizeof(kvmessage_t));
        server->pending_request->type = reqmsg->type;
        server->pending_request->message = NULL;
        server->pending_request->key = (char*) malloc(strlen(reqmsg->key)+1);
        strcpy(server->pending_request->key, reqmsg->key);
        if (reqmsg->value) {
          server->pending_request->value = (char*) malloc(strlen(reqmsg->value)+1);
          strcpy(server->pending_request->value, reqmsg->value);
        } else {
          server->pending_request->value = NULL;
        }
      }
      pthread_mutex_unlock(&server->pending_request_lock);
      return;

    case ABORT: 
      /* Log the ABORT */
      //check if we already logged the abort if we voted abort, if not, then we log it. 
      respmsg->type = ACK;
      if (server->pending_request) {
        tpclog_log(&server->log, ABORT, NULL, NULL);
        kvmessage_free(server->pending_request);
        server->pending_request = NULL;
      }
      return;

    case COMMIT:
      /* If server->pending_request is NULL, we know we have already handled
       * commiting the message, and this COMMIT is a duplicate COMMIT from the master.
       * So we just send an ACK. */
      if (!server->pending_request) {
        respmsg->type = ACK;
        return;
      }
      /* Handle the original request (stored in pending_request), by 
       * PUTing/DELing from the cache and store. */
      if (server->pending_request->type == PUTREQ) {
        err = kvserver_put(server, server->pending_request->key, 
                           server->pending_request->value);
      } else if (server->pending_request->type == DELREQ) {
        err = kvserver_del(server, server->pending_request->key);
      }
      if (err) {
        /* If PUT/DEL failed on the kvserver, do not commit and do not ACK.
         * The master should resend a COMMIT and we will try again next time. */
        respmsg->message = GETMSG(err);
        return;
      }

      /* Log the commit */
      tpclog_log(&server->log, COMMIT, server->pending_request->key, 
                 server->pending_request->value);
      respmsg->type = ACK;
      if (server->pending_request) {
        kvmessage_free(server->pending_request);
        server->pending_request = NULL;
      }
      return;
  }
}

/* Handles an incoming kvmessage REQMSG, and populates the appropriate fields
 * of RESPMSG as a response. RESPMSG and REQMSG both must point to valid
 * kvmessage_t structs. Assumes that the request should be handled as a non-TPC
 * message. See the spec for details on logic and error messages. */
void kvserver_handle_no_tpc(kvserver_t *server, kvmessage_t *reqmsg,
    kvmessage_t *respmsg) {
  int err;
  switch (reqmsg->type) {
    case GETREQ:
      err = kvserver_get(server, reqmsg->key, &respmsg->value); 
      respmsg->key = reqmsg->key;
      respmsg->type = GETRESP;
      break;
    case PUTREQ:
      err = kvserver_put(server, reqmsg->key, reqmsg->value);
      respmsg->type = RESP;
      break;
    case DELREQ:
      err = kvserver_del(server, reqmsg->key);
      respmsg->type = RESP;
      break;
    case INFO:
      respmsg->message = kvserver_get_info_message(server);
      respmsg->type = INFO;
      return;
    default:
      // invalid request
      respmsg->message = ERRMSG_INVALID_REQUEST;
      respmsg->type = RESP;
      return;
  }

  if (err) {
    respmsg->type = RESP;
    respmsg->message = GETMSG(err);
  } else {
    respmsg->message = MSG_SUCCESS;
  }
}

/* Generic entrypoint for this SERVER. Takes in a socket on SOCKFD, which
 * should already be connected to an incoming request. Processes the request
 * and sends back a response message.  This should call out to the appropriate
 * internal handler. */
void kvserver_handle(kvserver_t *server, int sockfd, void *extra) {
  kvmessage_t *reqmsg, *respmsg;
  respmsg = calloc(1, sizeof(kvmessage_t));
  reqmsg = kvmessage_parse(sockfd);
  void (*server_handler)(kvserver_t *server, kvmessage_t *reqmsg,
      kvmessage_t *respmsg);
  server_handler = server->use_tpc ?
    kvserver_handle_tpc : kvserver_handle_no_tpc;
  if (reqmsg == NULL) {
    respmsg->type = RESP;
    respmsg->message = ERRMSG_INVALID_REQUEST;
  } else {
    server_handler(server, reqmsg, respmsg);
  }
  kvmessage_send(respmsg, sockfd);
  if (respmsg->value != NULL) 
    free(respmsg->value);
  if (respmsg != NULL)
    free(respmsg);
  if (reqmsg != NULL)
    kvmessage_free(reqmsg);
}

/* Restore SERVER back to the state it should be in, according to the
 * associated LOG.  Must be called on an initialized  SERVER. Only restores the
 * state of the most recent TPC transaction, assuming that all previous actions
 * have been written to persistent storage. Should restore SERVER to its exact
 * state; e.g. if SERVER had written into its log that it received a PUTREQ but
 * no corresponding COMMIT/ABORT, after calling this function SERVER should
 * again be waiting for a COMMIT/ABORT.  This should also ensure that as soon
 * as a server logs a COMMIT, even if it crashes immediately after (before the
 * KVStore has a chance to write to disk), the COMMIT will be finished upon
 * rebuild. The cache need not be the same as before rebuilding.
 *
 * Checkpoint 2 only. 
 *
 * In kvserver_handle_tpc, we clear the log right before writing a new PUTREQ or
 * DELREQ to the log.  Thus the log will either have no elements (this only happens
 * if we haven't called any TPC requests at all yet), one element (a PUTREQ or DELREQ)
 * or two elements (a PUTREQ/DELREQ and a COMMIT/ABORT).  
 * For no elements, do nothing.
 * For one element (a PUTREQ/DELREQ), restore the state of pending_request, so that
 * the transaction can be completed.
 * For two elements, we check if the transaction is in the store, and if not, add it
 * to the store.
 *
 * The implementation of kvserver_rebuild_state below uses a while loop to handle
 * any number of entries in the log, which would be useful if, for example, completed
 * transactions do not immediately go to the store.  In the case of this project,
 * the loop would only do one iteration, since the log cannot contain more than 2 entries.
  */
 int kvserver_rebuild_state(kvserver_t *server) {
  tpclog_iterate_begin(&server->log); 

  logentry_t *entry1, *entry2;
  char key[1024];
  char value[1024];
  /* Check the log and restore the store any fully-completed commits. */
  while(tpclog_iterate_has_next(&server->log)) {
    entry1 = tpclog_iterate_next(&server->log);
    if (entry1->type == PUTREQ || entry1->type == DELREQ) {
      /* Parse the key and value from entry1. */
      /* entry->data is stored as [key_string\0value_string\0]. */
      int key_len = strlen(entry1->data)+1;
      int val_len = 0;
      strncpy(key, entry1->data, key_len);
      if (entry1->type == PUTREQ) {
        /* For PUT requests, we also need to copy the value from the data */
        val_len = strlen(entry1->data + key_len)+1;
        strncpy(value, entry1->data+key_len, val_len);
      }

      if (!tpclog_iterate_has_next(&server->log)) {
        /* entry1 is the last entry of the log, and it is a PUTREQ or DELREQ. 
         * So we must rebuild pending_request */
        server->pending_request = (kvmessage_t*) malloc(sizeof(kvmessage_t));
        server->pending_request->type = entry1->type;
        server->pending_request->message = NULL;
        server->pending_request->key = (char*) malloc(key_len);
        strncpy(server->pending_request->key, key, key_len);
        if (entry1->type == PUTREQ) {
          server->pending_request->value = (char*) malloc(val_len);
          strncpy(server->pending_request->value, value, val_len);
        } else {
          server->pending_request->value = NULL;
        }
        free(entry1);
        return 0;
      }

      /* Get the next entry (which should be a COMMIT or ABORT)
       * and check that this transaction is in the log. */
      entry2 = tpclog_iterate_next(&server->log);
      if (entry2->type == ABORT) {
        /* If the transaction was aborted, it was never written to the store. 
         * Thus we do not have to do anything further. */
        free(entry1);
        free(entry2);
        continue;
      } else {
        /* entry2 is a COMMIT.  Check if entry1 is a PUTREQ or a DELREQ. */
        if (entry1->type == PUTREQ) {
          char* store_val;
          int err = kvstore_get(&server->store, key, &store_val);
          /* PUTREQ: If the key-value pair is not in the store, put it in the store. */
          if (err || strcmp(store_val, value) != 0) {
            kvstore_put(&server->store, key, value);
          }
          free(entry1);
          free(entry2);
          continue;
        }
        if (entry1->type == DELREQ) {
          char* store_val;
          int err = kvstore_get(&server->store, key, &store_val);
          /* DELREQ: If the value is in the store, remove it from the store. */
          if (!err) {
            kvstore_del(&server->store, key);
          }
          free(entry1);
          free(entry2);
          continue;
        }
      }
    }
  }
  return 0;
}

/* Deletes all current entries in SERVER's store and removes the store
 * directory.  Also cleans the associated log. */
int kvserver_clean(kvserver_t *server) {
  return kvstore_clean(&server->store);
}
