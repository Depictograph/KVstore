#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netdb.h>
#include "kvconstants.h"
#include "kvmessage.h"
#include "socket_server.h"
#include "time.h"
#include "tpcmaster.h"
#include "utlist.h"
#include <pthread.h>

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
  pthread_rwlock_init(&master->tpc_lock, NULL);
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


/* Comparison function used to keep master's list of slaves in sorted order. Returns -1, 0, or 1 if a's id is less than, equal to
 * or greater than b's id.
*/
int slave_id_cmp(tpcslave_t *a, tpcslave_t *b) {
  int64_t a_id = a->id;
  int64_t b_id = b->id;
  if (a_id < b_id)
    return -1; 
  return a_id > b_id;
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
  respmsg->type = RESP;
  pthread_rwlock_rdlock(&master->slave_lock);
  if (master->slave_count >= master->slave_capacity) {
    respmsg->message = ERRMSG_GENERIC_ERROR;
    pthread_rwlock_unlock(&master->slave_lock);
    return;
  }
  pthread_rwlock_unlock(&master->slave_lock);
  tpcslave_t *slave = (tpcslave_t *) malloc(sizeof(tpcslave_t));
  pthread_rwlock_init(&slave->tpc_slave_lock, NULL);
  slave->host = (char*) malloc(strlen(reqmsg->key)+1);
  strcpy(slave->host, reqmsg->key);
  slave->port = (unsigned int) atoi(reqmsg->value);
  char *idstring = calloc(0, sizeof(slave->host) + sizeof(slave->port) + 2);
  if (!idstring) {
    respmsg->message = ERRMSG_GENERIC_ERROR;
    free(slave->host);
    free(slave); 
    free(reqmsg->value);
    return; 
  }
  memcpy(idstring, reqmsg->value, sizeof(slave->port));

  strcat(idstring, ":");
  strcat(idstring, slave->host);

  int64_t id = (int64_t) hash_64_bit(idstring);
  free(idstring);
  slave->id = id;
  tpcslave_t *elt; 
  pthread_rwlock_wrlock(&master->slave_lock);
  CDL_SEARCH_SCALAR(master->slaves_head, elt, id, slave->id);
  if (!elt) {
    CDL_PREPEND(master->slaves_head, slave);
    CDL_SORT(master->slaves_head, slave_id_cmp);
    master->slave_count += 1;
  } else {
    /* If the slave already was registered, do not add it to the list again, and just return success. */
    free(slave->host);
    free(slave);
  }
  respmsg->message = MSG_SUCCESS;
  pthread_rwlock_unlock(&master->slave_lock);
}

/* Hashes KEY and finds the first slave that should contain it.
 * It should return the first slave whose ID is greater than the
 * KEY's hash, and the one with lowest ID if none matches the
 * requirement.
 *
 * Checkpoint 2 only. 
 */
tpcslave_t *tpcmaster_get_primary(tpcmaster_t *master, char *key) {
  int64_t id = (int64_t) hash_64_bit(key);
  tpcslave_t *found_slave = NULL;
  pthread_rwlock_rdlock(&master->slave_lock);
  tpcslave_t *curr_slave = master->slaves_head;

  /* We keep the slaves sorted in increasing order by ID.  So just return
     the first slave with an ID greater than the key's hash value */
  CDL_FOREACH(master->slaves_head, curr_slave) {
    if (curr_slave->id >= id) {
      found_slave = curr_slave;
      break;
    } 
  }
  
  if (found_slave == NULL) {
    /* Could not find such a slave.  Return the one with the lowest ID. 
       If master has no registered slaves, tpcmaster_get_primary will
       return NULL.  */
    found_slave = master->slaves_head; 
  }
  pthread_rwlock_unlock(&master->slave_lock);
  return found_slave;
}

/* Returns the slave whose ID comes after PREDECESSOR's, sorted
 * in increasing order.
 *
 * Checkpoint 2 only. */
tpcslave_t *tpcmaster_get_successor(tpcmaster_t *master,
    tpcslave_t *predecessor) {
  tpcslave_t *elt;
  pthread_rwlock_rdlock(&master->slave_lock);
  CDL_SEARCH_SCALAR(master->slaves_head, elt, id, predecessor->id);
  // Check if predecessor is actually in the slave list of master. If it is not, return NULL. 
  if (elt != predecessor) {
    pthread_rwlock_unlock(&master->slave_lock);
    return NULL;
  }
  elt = predecessor->next; 
  if (!elt) {
    elt = master->slaves_head;
  }
  pthread_rwlock_unlock(&master->slave_lock);
  return elt; 

}

/* Handles an incoming GET request REQMSG, and populates the appropriate fields
 * of RESPMSG as a response. RESPMSG and REQMSG both must point to valid
 * kvmessage_t structs.
 *
 * Checkpoint 2 only. */
void tpcmaster_handle_get(tpcmaster_t *master, kvmessage_t *reqmsg,
    kvmessage_t *respmsg) {
  respmsg->type = RESP;
  /* If the key is too long, immediately return error */
  if (strlen(reqmsg->key) > MAX_KEYLEN) {
    respmsg->message = ERRMSG_KEY_LEN;
    return;
  }
  /* First check if the key is in the cache.  If so, just return it. */
  pthread_rwlock_t *cache_lock = kvcache_getlock(&master->cache, reqmsg->key);
  /* Note that kvcache_getlock cannot fail, since the only failure condition is
   * the key is too long. */ 
  pthread_rwlock_rdlock(cache_lock);
  int err = kvcache_get(&master->cache, reqmsg->key, &respmsg->value);
  pthread_rwlock_unlock(cache_lock);
  if (!err) {
    /* Found the element in the cache, just return it */
    respmsg->key = (char*) malloc(strlen(reqmsg->key)+1);
    strcpy(respmsg->key, reqmsg->key);
    respmsg->type = GETRESP;
    return;
  }
  /* At this point, the key is not in the cache, so forward the request to 
   * one of the corresponding slaves. */
  tpcslave_t *slave; // A kvserver which holds our value
  int sockfd;
  int redundancy = master->redundancy;
  int count = 0; // counter for issuing requests to slaves.  We stop once we talk to redundancy number of slaves.
  kvmessage_t *slave_resp;
  slave = tpcmaster_get_primary(master, reqmsg->key);
  if (!slave) {
    respmsg->message = ERRMSG_GENERIC_ERROR;
    return;
  }

  do {
    if (count > 0) {
      slave = tpcmaster_get_successor(master, slave);
    }
    pthread_rwlock_rdlock(&slave->tpc_slave_lock);
    sockfd = connect_to(slave->host, slave->port, 2);
    if (sockfd == -1) {
      count++;
      pthread_rwlock_unlock(&slave->tpc_slave_lock);
      continue;
    }
    kvmessage_send(reqmsg, sockfd);
    slave_resp = kvmessage_parse(sockfd);
    close(sockfd);  
    if (!slave_resp || slave_resp->type != GETRESP) {
      if(slave_resp) {
        kvmessage_free(slave_resp);
      }
      count++;
      pthread_rwlock_unlock(&slave->tpc_slave_lock);
      continue;
    }
    /* Found a slave with the value we wanted.  Put the value in the cache
     * and then return success */
    respmsg->type = GETRESP;
    respmsg->key = (char*) malloc(strlen(reqmsg->key)+1);
    strcpy(respmsg->key, reqmsg->key);
    respmsg->value = (char*) malloc(strlen(slave_resp->value)+1);
    memcpy(respmsg->value, slave_resp->value, strlen(slave_resp->value)+1);
    kvmessage_free(slave_resp);
    pthread_rwlock_unlock(&slave->tpc_slave_lock);

    /* We do not care if kvcache_put errors, since it is a cache. */
    pthread_rwlock_rdlock(cache_lock);
    int err = kvcache_put(&master->cache, respmsg->key, respmsg->value);
    pthread_rwlock_unlock(cache_lock);
    return;
  } while (count < redundancy);
  /* If we did not successfully find a slave with the value, return an error. */
  respmsg->message = ERRMSG_NO_KEY;
  return;
}
   
/* Handles errors in tpcmaster_handle_tpc. 
 * The respmsg->type should be set to RESP by the caller of this function. 
 * The caller must be holding the master->tpc_lock. */
void tpcmaster_handle_tpc_error(tpcmaster_t *master, kvmessage_t *respmsg, 
    char *error_msg) {
  respmsg->message = error_msg;
  pthread_rwlock_unlock(&master->tpc_lock);  
  return;
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
 * Checkpoint 2 only. 
 *
 * We do not need to acquire master->slave_lock since the list of slaves must be completed
 * before handling tpc requests, so the list cannot change by the time we call this function.*/
void tpcmaster_handle_tpc(tpcmaster_t *master, kvmessage_t *reqmsg,
    kvmessage_t *respmsg, callback_t callback) {
  respmsg->type = RESP;
  int redundancy = master->redundancy;
  /* If the key or value is too long, immediately return an error message */
  if (strlen(reqmsg->key) > MAX_KEYLEN) {
    respmsg->message = ERRMSG_KEY_LEN;
    return;
  }
  if (reqmsg->type == PUTREQ && strlen(reqmsg->value) > MAX_VALLEN) {
    respmsg->message = ERRMSG_VAL_LEN;
    return;
  }

  pthread_rwlock_wrlock(&master->tpc_lock); // Lock to prevent another tpc transaction. 
  /* If not all slaves have been registered, drop the request */
  pthread_rwlock_rdlock(&master->slave_lock);
  if (master->slave_count < master->slave_capacity) {
    tpcmaster_handle_tpc_error(master, respmsg, ERRMSG_GENERIC_ERROR);
    pthread_rwlock_unlock(&master->slave_lock);
    return;
  }
  /* Once we established that all the slaves are registered, we can safely assume
   * the slave list will never change.  So we no longer need the lock. */
  pthread_rwlock_unlock(&master->slave_lock);
  
  /* Create an array of tpcslave_t* so that we only need to call
   * tpcmaster_get_primary and tpcmaster_get_successor once for each slave. */  
  tpcslave_t* tpc_slaves[redundancy]; // list of slaves
  tpc_slaves[0] = tpcmaster_get_primary(master, reqmsg->key);
  if (!tpc_slaves[0]) { 
    tpcmaster_handle_tpc_error(master, respmsg, ERRMSG_GENERIC_ERROR);
    return;
  }
  int i;
  for(i = 1; i<redundancy; i++) {
    tpc_slaves[i] = tpcmaster_get_successor(master, tpc_slaves[i-1]);
    if (!tpc_slaves[i]) { 
      tpcmaster_handle_tpc_error(master, respmsg, ERRMSG_GENERIC_ERROR);
      return;
    }
  }
  /* Phase 1.
   * Forward the PUT/DEL request message to each slave, and wait for a vote commit/abort. 
   * Also acquire the slaves lock right before we send the message, and hold the lock until
   * we receive the ACK from that slave. */
  int sockfd;
  kvmessage_t *slave_resp;
  int vote_commit = 1; // Flag for commit or abort.  If receive abort or timeout, set to 0.
  for(i = 0; i<redundancy; i++) {
    /* Forward the request message to the slave. */
    pthread_rwlock_wrlock(&(tpc_slaves[i]->tpc_slave_lock));
    sockfd = connect_to(tpc_slaves[i]->host, tpc_slaves[i]->port, 2);
    if (sockfd == -1) {
      if (callback)
        callback(tpc_slaves[i]);
      vote_commit = 0;
      continue;
    }
    kvmessage_send(reqmsg, sockfd);
    /* kvmessage_parse will call read, which blocks until the server sends a response. 
     * read has a timeout of 2 seconds (the argument passed into connect_to above), 
     * after which kvmessage_parse will return NULL, signaling the master to ABORT. */
    slave_resp = kvmessage_parse(sockfd);
    close(sockfd);  
    if (!slave_resp || slave_resp->type != VOTE_COMMIT) {
      vote_commit = 0; // We will abort, but we still contact the rest of the slaves.
    }
    kvmessage_free(slave_resp);
  } 
  
  if (callback) {
    callback(NULL); 
  }
  /* Phase 2.  Send global commit or abort to all slaves.  We need to make a new
   * reqmsg containing the global COMMIT or ABORT command. */ 
  kvmessage_t global_req;
  memset(&global_req, 0, sizeof(kvmessage_t));
  if (vote_commit) {
    // Make the COMMIT command message.
    global_req.type = COMMIT;
  } else {
    // Make the ABORT command message.
    global_req.type = ABORT;
  }

  // Send the COMMIT/ABORT command to each slave, one at a time, waiting until we get an ACK
  i = 0;
  while(i < redundancy) {  
    /* Get a new socket to the slave */
    do {  
      sockfd = connect_to(tpc_slaves[i]->host, tpc_slaves[i]->port, 2);
      if (sockfd == -1) {
        if (callback)
          callback(tpc_slaves[i]);
        sleep(1);
      }
    } while (sockfd == -1);
    kvmessage_send(&global_req, sockfd);
    slave_resp = kvmessage_parse(sockfd);
    if (!slave_resp || slave_resp->type != ACK) {
      close(sockfd);
      kvmessage_free(slave_resp);
      continue;
    }
    close(sockfd);
    kvmessage_free(slave_resp);
    // Now we can safely release this slave's lock 
    pthread_rwlock_unlock(&(tpc_slaves[i]->tpc_slave_lock));
    i++;
  }
  /* Release the tpc lock and return a success or error response to the client */ 
  pthread_rwlock_unlock(&master->tpc_lock);
  if (vote_commit) {
    /* If the PUT/DEL request was successful, also put it in the master's cache */
    respmsg->message = MSG_SUCCESS;
    pthread_rwlock_t* cache_lock = kvcache_getlock(&master->cache, reqmsg->key);
    pthread_rwlock_wrlock(cache_lock);
    if (reqmsg->type == PUTREQ) {
      kvcache_put(&master->cache, reqmsg->key, reqmsg->value);
    } else {
      kvcache_del(&master->cache, reqmsg->key);
    }
    pthread_rwlock_unlock(cache_lock);
  } else {
    respmsg->message = ERRMSG_GENERIC_ERROR;
  }
  return;
}

/* Returns an info string about MASTER including timestamp, and a list of
 * slaves with hostname and ports. Does this by trying to connect_to each
 * slave, and adding a {HOSTNAME, PORT} line if connect_to was successful. */
char *tpcmaster_get_info_message(tpcmaster_t *master) {
  char info[2048], buf[256];
  time_t ltime = time(NULL);
  strcpy(info, asctime(localtime(&ltime)));
  sprintf(buf, "Slaves:");
  strcat(info, buf);
  pthread_rwlock_rdlock(&master->slave_lock);
  int sockfd;
  int i;
  tpcslave_t* slave = master->slaves_head; 
  for(i = 0; i < master->slave_count; i++) {
    /* Try to connect_to the slave. */
    sockfd = connect_to(slave->host, slave->port, 2);
    if (sockfd != -1) {
      /* Write a line to response containing {HOSTNAME, PORT} */
      sprintf(buf, "\n{%s, %d}", slave->host, slave->port);
      strcat(info, buf);
    }
    close(sockfd);
    slave = slave->next;
  }
  pthread_rwlock_unlock(&master->slave_lock);
  char *msg = malloc(strlen(info));
  strcpy(msg, info);
  return msg;
}

/* Handles an incoming kvmessage REQMSG, and populates the appropriate fields
 * of RESPMSG as a response. RESPMSG and REQMSG both must point to valid
 * kvmessage_t structs. Provides information about the slaves that are
 * currently alive.
 *
 * Checkpoint 2 only. */
void tpcmaster_info(tpcmaster_t *master, kvmessage_t *reqmsg,
    kvmessage_t *respmsg) {
  respmsg->type = INFO; 
  respmsg->message = tpcmaster_get_info_message(master);
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
  // Do not need this section since we always malloc and copy the key in our functions
  // if (reqmsg->key != NULL) {
  //   respmsg.key = calloc(1, strlen(reqmsg->key));
  //   strcpy(respmsg.key, reqmsg->key);
  // }
  // Can reqmsg be NULL?  Shouldn't this if case come after the one below?
  if (reqmsg->type == INFO) {
    tpcmaster_info(master, reqmsg, &respmsg);
    kvmessage_send(&respmsg, sockfd);
    free(respmsg.message);
    return;
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
  if (respmsg.key != NULL) {
    free(respmsg.key);
  }
  if (respmsg.value != NULL)
    free(respmsg.value);
}

/* Completely clears this TPCMaster's cache. For testing purposes. */
void tpcmaster_clear_cache(tpcmaster_t *tpcmaster) {
  kvcache_clear(&tpcmaster->cache);
}
