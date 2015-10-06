#include <pthread.h>
#include <errno.h>
#include <stdbool.h>
#include "uthash.h"
#include "utlist.h"
#include "kvconstants.h"
#include "kvcacheset.h"
#include <assert.h>

/* Initializes CACHESET to hold a maximum of ELEM_PER_SET elements.
 * ELEM_PER_SET must be at least 2.
 * Returns 0 if successful, else a negative error code. */
int kvcacheset_init(kvcacheset_t *cacheset, unsigned int elem_per_set) {
  int ret;
  cacheset->entries = NULL;
  cacheset->num_entries = 0;

  if (elem_per_set < 2)
    return -1;
  cacheset->elem_per_set = elem_per_set;
  if ((ret = pthread_rwlock_init(&cacheset->lock, NULL)) < 0)
    return ret;
  return 0;
}


/* Get the entry corresponding to KEY from CACHESET. Returns 0 if successful,
 * else returns a negative error code. If successful, populates VALUE with a
 * malloced string which should later be freed. */
int kvcacheset_get(kvcacheset_t *cacheset, char *key, char **value) {
  struct kvcacheentry* entry; 
  //pthread_rwlock_rdlock(&cacheset->lock); 
  HASH_FIND_STR(cacheset->entries, key, entry);
  //pthread_rwlock_unlock(&cacheset->lock);
  if (entry == NULL) {
    *value = NULL;
    return ERRNOKEY;
  }
  entry->refbit = true;
  *value = malloc(strlen(entry->value)+1);
  strcpy(*value, entry->value);
  return 0;
}

void delete_entry(kvcacheset_t *cacheset, struct kvcacheentry *entry) {
  HASH_DEL(cacheset->entries, entry);  
  cacheset->num_entries--;
  free(entry->key);
  free(entry->value);
  free(entry);
}

/* Evicts a old entry in the cacheset using the second chance eviction policy. Caller must be holding a write lock when
 * this function is called. 
*/
int kvcacheset_evict(kvcacheset_t *cacheset) {
  if (cacheset->num_entries == 0) {
    return -1;
  }
  struct kvcacheentry *entry;
  // look at the head of the queue
  entry = cacheset->entries;
  while (entry->refbit) {
    entry->refbit = false;
    // Move head to the back of the queue
    HASH_DEL(cacheset->entries, entry);
    HASH_ADD_KEYPTR(hh, cacheset->entries, entry->key, strlen(entry->key), entry);  
    // Update the pointer to look at the next head
    entry = cacheset->entries;
  }
  // Found entry with refbit false, evict it
  delete_entry(cacheset, entry);
  return 0;
}

/* Add the given KEY, VALUE pair to CACHESET. Returns 0 if successful, else
 * returns a negative error code. Should evict elements if necessary to not
 * exceed CACHESET->elem_per_set total entries. */
int kvcacheset_put(kvcacheset_t *cacheset, char *key, char *value) {
  struct kvcacheentry *entry, *newentry;

  //pthread_rwlock_rdlock(&cacheset->lock); 
  HASH_FIND_STR(cacheset->entries, key, entry);
  //pthread_rwlock_unlock(&cacheset->lock);

  //pthread_rwlock_wrlock(&cacheset->lock);
  if (entry == NULL) {
    if (cacheset->num_entries == cacheset->elem_per_set) {
      if (kvcacheset_evict(cacheset) != 0) {
        assert(false);
      } 
    }
  } else {
    entry->refbit = true;
    free(entry->value);
    entry->value = malloc(strlen(value + 1));
    strcpy(entry->value, value); 
    //pthread_rwlock_unlock(&cacheset->lock);
    return 0;
  }
  newentry = (struct kvcacheentry *) malloc(sizeof(struct kvcacheentry));
  newentry->key = malloc(strlen(key) + 1);
  strcpy(newentry->key, key); 
  newentry->value = malloc(strlen(value) + 1);
  strcpy(newentry->value, value); 
  newentry->refbit = false; 
  HASH_ADD_KEYPTR(hh, cacheset->entries, newentry->key, strlen(newentry->key), newentry);  
  cacheset->num_entries++;

  //pthread_rwlock_unlock(&cacheset->lock);
  return 0;
}

/* Deletes the entry corresponding to KEY from CACHESET. Returns 0 if
 * successful, else returns a negative error code. */
int kvcacheset_del(kvcacheset_t *cacheset, char *key) {
  struct kvcacheentry* entry; 
  //pthread_rwlock_rdlock(&cacheset->lock); 
  HASH_FIND_STR(cacheset->entries, key, entry);
  //pthread_rwlock_unlock(&cacheset->lock);
  if (entry == NULL) {
    return ERRNOKEY;
  }
  //pthread_rwlock_wrlock(&cacheset->lock);
  delete_entry(cacheset, entry);
  //pthread_rwlock_unlock(&cacheset->lock);
  return 0;
}

/* Completely clears this cache set. For testing purposes. */
void kvcacheset_clear(kvcacheset_t *cacheset) {
  struct kvcacheentry *entry, *tmp;
  //pthread_rwlock_wrlock(&cacheset->lock);
  HASH_ITER(hh, cacheset->entries, entry, tmp) {
    delete_entry(cacheset, entry);
  }
  assert(cacheset->num_entries == 0);
  //pthread_rwlock_unlock(&cacheset->lock);
}
