#include <pthread.h>
#include <errno.h>
#include <stdbool.h>
#include "uthash.h"
#include "utlist.h"
#include "kvconstants.h"
#include "kvcacheset.h"
#include <string.h>

/* Initializes CACHESET to hold a maximum of ELEM_PER_SET elements.
 * ELEM_PER_SET must be at least 2.
 * Returns 0 if successful, else a negative error code. */
int kvcacheset_init(kvcacheset_t *cacheset, unsigned int elem_per_set) {
    int ret;
    if (elem_per_set < 2)
        return -1;
    cacheset->elem_per_set = elem_per_set;
    if ((ret = pthread_rwlock_init(&cacheset->lock, NULL)) < 0)
        return ret;
    cacheset->num_entries = 0;
    cacheset->last = NULL;
    cacheset->first = NULL;
    return 0;
}


/* Get the entry corresponding to KEY from CACHESET. Returns 0 if successful,
 * else returns a negative error code. If successful, populates VALUE with a
 * malloced string which should later be freed. */
int kvcacheset_get(kvcacheset_t *cacheset, char *key, char **value) {
    //printf("in get.\n");
    struct kvcacheentry *temp = cacheset->last;
    if (!temp) {
        return ERRNOKEY;
    }
    int i;
    for(i=0; i<cacheset->num_entries; i++) {
        if(strcmp(temp->key, key)==0) {
            //printf("h1 - %s - %s - %s\n", temp->key, key, temp->value);
            *value = malloc(sizeof(temp->value));
            strcpy(*value, temp->value);
            temp->refbit = true;
            return 0;
        }
        if(temp->prev==NULL) {
            return ERRNOKEY;
        } else {
            temp = temp->prev;
        }
    }
    return ERRNOKEY;
}

/* Add the given KEY, VALUE pair to CACHESET. Returns 0 if successful, else
 * returns a negative error code. Should evict elements if necessary to not
 * exceed CACHESET->elem_per_set total entries. */
int kvcacheset_put(kvcacheset_t *cacheset, char *key, char *value) {
    // check if the size fits
    if ((cacheset->num_entries)==(cacheset->elem_per_set)) {
        /*iterate through each entry, beginning at the first and evict the first one with refbit false. */
        int i;
        struct kvcacheentry *temp1 = cacheset->first;
        for(i=0; i<cacheset->num_entries; i++) {
            if (temp1->refbit) {
                temp1->refbit = false;
                temp1 = temp1->next;
                if(!temp1) {
                    i=0;
                    temp1 = cacheset->first;
                }
            } else {
                kvcacheset_del(cacheset, temp1->key);
                break;
            }
        }
    }
    
    int i;
    struct kvcacheentry *temp1 = cacheset->first;
    for(i=0; i<cacheset->num_entries; i++) {
        if (strcmp(temp1->key, key)==0) {
            temp1->value = realloc(temp1->value, sizeof(value));
            strcpy(temp1->value, value);
            return 0;
        }
    }
    
    //printf("hello!!\n");
    struct kvcacheentry* temp = malloc(sizeof(struct kvcacheentry));
    //printf("hello2!!%d %d %s\n", sizeof(key), sizeof(*key), key);
    temp->key = malloc(sizeof(key));
    strcpy(temp->key, key);
    temp->value = malloc(sizeof(value));
    strcpy(temp->value, value);
    //printf("hello3!! %s\n", temp->key);
    temp->refbit = false;
    temp->next = NULL;
    
    if(cacheset->num_entries==0) {
        temp->prev = NULL;
        cacheset->first = temp;
        cacheset->last = temp;
    } else {
        temp->prev = cacheset->last;
        cacheset->last->next = temp;
    }
    cacheset->last = temp;
    cacheset->num_entries += 1;
    //printf("reached end of put successfully.\n");
    return 0;
}

/* Deletes the entry corresponding to KEY from CACHESET. Returns 0 if
 * successful, else returns a negative error code. */
int kvcacheset_del(kvcacheset_t *cacheset, char *key) {
    int i;
    struct kvcacheentry *temp1 = cacheset->first;
    for(i=0; i<cacheset->num_entries; i++) {
        if (!temp1) {
            return -1;
        }
        if (strcmp(temp1->key, key)==0) {
            if (temp1->prev) {
                //printf("%s\n", temp1->prev->key);
                temp1->prev->next=temp1->next;
            }
            if (temp1->next) {
                temp1->next->prev = temp1->prev;
            }
            if (strcmp(temp1->key, cacheset->first->key)==0) {
                cacheset->first = temp1->next;
            }
            if (strcmp(temp1->key, cacheset->last->key)==0) {
                cacheset->last = temp1->prev;
            }
            cacheset->num_entries -= 1;
            free(temp1->key);
            free(temp1->value);
            free(temp1);
            //printf("reaches end of del.\n");
            return 0;
        } else {
            temp1 = temp1->next;
        }
    }
    return -1;
}

/* Completely clears this cache set. For testing purposes. */
void kvcacheset_clear(kvcacheset_t *cacheset) {
    int i;
    struct kvcacheentry *temp1 = cacheset->first;
    for(i=0; i<cacheset->num_entries; i++) {
        if(!temp1->next) {
            free(temp1->key);
            free(temp1->value);
            free(temp1);
            break;
        }
        temp1 = temp1->next;
        free(temp1->prev->key);
        free(temp1->prev->value);
        free(temp1->prev);
    }
    cacheset->num_entries = 0;
}