#ifndef BASE_H
#define BASE_H
#include"leveldb/include/leveldb/slice.h"
#include<cstdio>
#include<cstdint>
#include"leveldb/util/arena.h"
#include"sha1.h"

namespace PUF{
static const int kKiloByte = 1024;    
static const int kMegaByte = 1024*1024;
static const long kGigaByte = 1024*1024*1024;
static const uint16_t kSmallItemLen = 50;
static const uint16_t kSmallKeyLen = 20;
static const char SHORT_VALUE = 0;
static const char LONG_VALUE = 1;
class Record{
public:        
    // key_size(uint16_t) key(char *)
    leveldb::Slice key;
    // value is divided into two types
    // type 0 value_size(uint16_t) value(char *)
    // type 1 len_of_record(uint16_t) value_loc(uint64_t)
    leveldb::Slice value;

    Record(leveldb::Slice k, leveldb::Slice v):key(k),value(v){}
    Record(leveldb::Slice k, leveldb::Slice v, FILE *f):key(k),value(v),fp_(f){}

    void setFile(FILE *fp){
        fp_ = fp;
    }
    leveldb::Slice get_real_key(){
        uint16_t key_size;
        memcpy(&key_size,key.data(),2);
        return leveldb::Slice(key.data() + 2, key_size);
    }

    leveldb::Slice get_real_value(){
        char type;
        memcpy(&type,value.data(),1);
        uint16_t value_size;
        if (type == 0) {
            memcpy(&value_size,value.data() + 1,2);
            return leveldb::Slice(key.data() + 3, value_size);
        } else {
            memcpy(&value_size, value.data() + 1,2);
            uint64_t value_loc;
            memcpy(&value_loc, value.data() + 3,8);
            fseek(fp_,SEEK_SET,value_loc);

            fread(value_buf_,value_size,1,fp_);
            return leveldb::Slice(value_buf_,value_size);
        }
    }
private:
    FILE *fp_;    
    char value_buf_[kMegaByte];
};

enum class StatusCode{
    SUCCESS = 0,
    INVALID_KEY_SIZE,
    INVAILD_VALUE_SIZE,
    INVALID_KEY,
    INVALID_VALUE,
    INVALID_PARAMS,
    EOF_OF_FILE,
};

class FileIterator{
public:
    FileIterator(FILE *fp):fp_(fp),curr_(0){                
        //set buffer
    }
    // 0 means success
    // 1 means EOF
    StatusCode readRecordFromFile (Record *r) {
        if (r == nullptr) {
            return StatusCode::INVALID_PARAMS;
        }
        if (fread(&key_size_,8,1,fp_) == 0) {        
            if(feof(fp_)){
                return StatusCode::EOF_OF_FILE;
            }
            return StatusCode::INVALID_KEY_SIZE;        
        }
        curr_ += 8;

        if (key_size_ > kKiloByte) {
            return StatusCode::INVALID_KEY_SIZE;
        }
        char* key = arena_.Allocate(key_size_ + 2);
        uint16_t key_size = static_cast<uint16_t>(key_size_);
        if (key_size <= kSmallKeyLen){
            memcpy(key, &key_size, 2);
            if (fread(key + 2, key_size_, 1, fp_) == 0){
                return StatusCode::INVALID_KEY;
            }
        }else{
            memcpy(key, &kSmallKeyLen, 2);
            if (fread(key_buf_,key_size_,1,fp_) == 0){
                return StatusCode::INVALID_KEY;
            }
            //sha1 
            std::string key_sha = sha1(std::string(key_buf_,key_size_));
            memcpy(key + 2, key_sha.c_str(), 20);
            key_size = 20;
        }
        curr_ += key_size_;
        r->key = leveldb::Slice(key,key_size + 2);

        if (fread(&value_size_,8,1,fp_) == 0){
            return StatusCode::INVAILD_VALUE_SIZE;
        }
        if (value_size_ > kMegaByte){
            return StatusCode::INVAILD_VALUE_SIZE;
        }
        curr_ += 8;

        uint16_t value_size = static_cast<uint16_t>(value_size_);
        char* value = nullptr;
        if (value_size_ + key_size < kSmallItemLen) {
            if(fread(value_buf_,value_size_,1,fp_) == 0){
                return StatusCode::INVALID_VALUE;
            }
            //1 for type, 2 for value_size
            value = arena_.Allocate(value_size + 3);
            memcpy(value, &SHORT_VALUE, 1);
            memcpy(value + 1, &value_size, 2);
            memcpy(value + 3, value_buf_, value_size);            
        } else {
            uint16_t len_of_record = key_size + value_size;
            value_size = 8;
            //1 for type , 2 for len_of_record , 8 for uint64_t;
            value = arena_.Allocate(value_size + 3);
            memcpy(value, &LONG_VALUE, 1);
            memcpy(value + 1,&len_of_record,2);
            // 16 for key_size and value_size 
            uint64_t record_off = curr_ - 16 - key_size_;
            memcpy(value + 3,&record_off,8);
            fseek(fp_,value_size_,SEEK_CUR);
        }
        curr_ += value_size;        
        r->value = leveldb::Slice(value,value_size + 3);
    }

    StatusCode WriteToUnSortedFile(const Record &r){
        uint64_t len = r.key.size();
        if (fwrite(&len,8,1,fp_) == 0){            
            return StatusCode::INVALID_KEY_SIZE;
        }

        if(fwrite(r.key.data(),r.key.size(),1,fp_) == 0){
            return StatusCode::INVALID_KEY;
        }

        len = r.value.size();
        if (fwrite(&len,8,1,fp_) == 0) {
            return StatusCode::INVAILD_VALUE_SIZE;
        }
        if (fwrite(r.value.data(),len,1,fp_) == 0) {
            return StatusCode::INVALID_VALUE;
        }            
    }
    
private:
    FILE *fp_;
    off_t curr_;
    leveldb::Arena arena_;
    uint64_t key_size_;
    uint64_t value_size_;
    char value_buf_[kMegaByte];
    char key_buf_[kKiloByte];
};

}
//namespace PUF
#endif 

