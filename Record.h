#ifndef RECORD_H
#define RECORD_H
#include<base.h>
namespace PUF{
//record is read from unsorted file and processed by FileIterator
class Record{
public:           
    Record(){}
    Record(leveldb::Slice k, leveldb::Slice v):key(k),value(v){}

    // key_size(uint16_t) key(char *)
    leveldb::Slice key;
    // value is divided into two types
    // type 0 value_size(uint16_t) value(char *)
    // type 1 len_of_record(uint16_t) value_loc(uint64_t)
    leveldb::Slice value;

    size_t size() const {
        return key.size() + value.size();
    }

    static void setFile(FILE *fp) {
        fp_ = fp;
    }

    leveldb::Slice get_real_key() const {
        uint16_t key_size;
        memcpy(&key_size,key.data(),2);
        return leveldb::Slice(key.data() + 2, key_size);
    }

    leveldb::Slice get_real_value(){
        char type;
        memcpy(&type,value.data(),1);
        uint16_t value_size;
        if (type == SHORT_VALUE) {
            memcpy(&value_size,value.data() + 1,2);
            return leveldb::Slice(key.data() + 3, value_size);
        } else if (type == LONG_VALUE) {
            memcpy(&value_size, value.data() + 1,2);
            uint64_t value_loc;
            memcpy(&value_loc, value.data() + 3,8);
            fseek(fp_,SEEK_SET,value_loc);

            if(value_buf_ == nullptr){
                value_buf_ = (char *)zMalloc(kMegaByte + kKiloByte + 16);
            }

            fread(value_buf_,value_size,1,fp_);
            uint64_t key_size;
            memcpy(&key_size,value_buf_,8);
            uint64_t value_size_64;
            memcpy(&value_size_64,value_buf_ + 8 + key_size,8);

            return leveldb::Slice(value_buf_ + 16 + key_size,value_size_64);
        } else {
            std::cout<<"unknown value type"<<std::endl;
            exit(1);
        }
    }

    bool operator < (const Record& r1) const {
        return this->get_real_key().compare(r1.get_real_key());
    }

    ~Record(){
        if(value_buf_ != nullptr){
            zFree(value_buf_);
            value_buf_ = nullptr;
        }
        if(key.data() != nullptr){
            zFree(const_cast<char*>(key.data()));
        }
        if(value.data() != nullptr){
            zFree(const_cast<char*>(value.data()));
        }
        key = leveldb::Slice();
        value = leveldb::Slice();
    }

private:
    static FILE *fp_;    
    char* value_buf_;
  

};

typedef std::shared_ptr<Record> RecordPtr;
} //namespace PUF




#endif