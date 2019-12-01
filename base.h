#ifndef BASE_H
#define BASE_H
#include"leveldb/include/leveldb/slice.h"
#include<cstdio>
#include<cstdint>
#include"sha1.h"
#include <atomic>
#include<malloc/malloc.h>

namespace PUF{

std::atomic<bool>  has_records_in_unsorted_file(true);
static std::atomic<size_t> used_memory(0);
void inline update_zmalloc_stat_alloc(size_t n){
    used_memory.fetch_add(n,std::memory_order_relaxed);
}

void inline updatte_zmalloc_stat_free(size_t n){
    used_memory -= n;
}

void* zMalloc(size_t n){
    void* ptr = malloc(n);
    update_zmalloc_stat_alloc(n);
    return ptr;
}

void zFree(void* ptr){
    free(ptr);
    updatte_zmalloc_stat_free(malloc_size(ptr));
}

static const int kKiloByte = 1024;    
static const int kMegaByte = 1024*1024;
static const long kGigaByte = (long)1024*1024*1024;
static const uint16_t kSmallItemLen = 50;
static const uint16_t kSmallKeyLen = 20;
static const char SHORT_VALUE = 0;
static const char LONG_VALUE = 1;


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

