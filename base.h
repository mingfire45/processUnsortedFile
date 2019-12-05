#ifndef BASE_H
#define BASE_H
#include"leveldb/include/leveldb/slice.h"
#include<cstdio>
#include<cstdint>
#include"sha1.h"
#include <atomic>
#include <malloc.h>
#include <memory>

#ifdef HAVE_MALLOC_SIZE
#define PREFIX_SIZE (0)
#else
#if defined(__sun) || defined(__sparc) || defined(__sparc__)
#define PREFIX_SIZE (sizeof(long long))
#else
#define PREFIX_SIZE (sizeof(size_t))
#endif
#endif


namespace PUF{

static std::atomic<size_t> used_memory(0);
void inline update_zmalloc_stat_alloc(size_t _n){    
    if (_n&(sizeof(long)-1)) _n += sizeof(long)-(_n&(sizeof(long)-1)); 
    used_memory.fetch_add(_n,std::memory_order_relaxed);
}

void inline updatte_zmalloc_stat_free(size_t _n){
    if (_n&(sizeof(long)-1)) _n += sizeof(long)-(_n&(sizeof(long)-1)); \
    used_memory -= n;
}

static void zmalloc_default_oom(size_t size) {
    fprintf(stderr, "zmalloc: Out of memory trying to allocate %zu bytes\n",
        size);
    fflush(stderr);
    abort();
}

static void (*zmalloc_oom_handler)(size_t) = zmalloc_default_oom;

void *zMalloc(size_t size) {
    void *ptr = malloc(size+PREFIX_SIZE);

    if (!ptr) zmalloc_oom_handler(size);
#ifdef HAVE_MALLOC_SIZE
    update_zmalloc_stat_alloc(zmalloc_size(ptr));
    return ptr;
#else
    *((size_t*)ptr) = size;
    update_zmalloc_stat_alloc(size+PREFIX_SIZE);
    return (char*)ptr+PREFIX_SIZE;
#endif
}

void zFree(void *ptr) {
#ifndef HAVE_MALLOC_SIZE
    void *realptr;
    size_t oldsize;
#endif

    if (ptr == NULL) return;
#ifdef HAVE_MALLOC_SIZE
    update_zmalloc_stat_free(zmalloc_size(ptr));
    free(ptr);
#else
    realptr = (char*)ptr-PREFIX_SIZE;
    oldsize = *((size_t*)realptr);
    update_zmalloc_stat_free(oldsize+PREFIX_SIZE);
    free(realptr);
#endif
}


static const int kKiloByte = 1024;    
static const int kMegaByte = 1024*1024;
static const long kGigaByte = (long)1024*1024*1024;
static const uint16_t kSmallItemLen = 50;
static const uint16_t kSmallKeyLen = 20;
static const char SHORT_VALUE = 0;
static const char LONG_VALUE = 1;
std::atomic<bool>  has_records_in_unsorted_file(true);

enum class StatusCode{
    SUCCESS = 0,
    INVALID_KEY_SIZE,
    INVAILD_VALUE_SIZE,
    INVALID_KEY,
    INVALID_VALUE,
    INVALID_PARAMS,
    EOF_OF_FILE,
};



}
//namespace PUF
#endif 

