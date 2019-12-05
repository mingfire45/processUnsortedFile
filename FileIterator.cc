#include"FileIterator.h"
namespace PUF{

StatusCode FileIterator::readRecordFromUnsortedFile(RecordPtr &r) {
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
        char* key = (char *)zMalloc(key_size_ + 2);
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
            value = (char *)zMalloc(value_size + 3);
            memcpy(value, &SHORT_VALUE, 1);
            memcpy(value + 1, &value_size, 2);
            memcpy(value + 3, value_buf_, value_size);            
        } else {
            uint16_t len_of_record = key_size + value_size;
            value_size = 8;
            //1 for type , 2 for len_of_record , 8 for uint64_t;
            value = (char *)zMalloc(value_size + 3);
            memcpy(value, &LONG_VALUE, 1);
            memcpy(value + 1,&len_of_record,2);
            // 16 for key_size and value_size 
            uint64_t record_off = curr_ - 16 - key_size_;
            memcpy(value + 3,&record_off,8);
            fseek(fp_,value_size_,SEEK_CUR);
        }
        curr_ += value_size;        
        r->value = leveldb::Slice(value,value_size + 3);
        return StatusCode::SUCCESS;
}

StatusCode FileIterator::readRecordFromSortedFile(RecordPtr &r,char* key, char* value){
    uint16_t key_size;
    if (fread(&key_size, 2, 1, fp_) == 0) {
        return StatusCode::INVALID_KEY_SIZE;
    }

    if (fread(key + 2, key_size, 1, fp_) == 0){
        return StatusCode::INVAILD_VALUE_SIZE;
    }
    memcpy(key,&key_size,2);

    char value_type;
    if (fread(&value_type,1,1,fp_) == 0){
        return StatusCode::INVALID_PARAMS;
    }

    uint16_t value_size;
    if（value_type == SHORT_VALUE) {
        if (fread(&value_size,2,1,fp_) == 0){
            return StatusCode::INVALID_VALUE_SIZE;
        }
        if (fread(value + 3,value_size, 1, fp_) == 0){
            return StatusCode::INVALID_VALUE;
        }
        memcpy(value, SHORT_VALUE, 1);
        memcpy(value+1,&value_size, 2);
    } else {
        if (fread(value+1,2+8,1,fp__) == 0){
            return StatusCode::INVALID_VALUE;
        }
        memcpy(value,LONG_VALUE,1);
    }

}

StatusCode FileIterator::writeToUnSortedFile(const Record &r){
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
    return StatusCode::SUCCESS;     
}

StatusCode FileIterator::writeToSortedFile(const Record &r){
    if (fwrite(r.key.data(),r.key.size(),1,fp_) == 0){
        return StatusCode::INVALID_KEY;
    }

    if (fwrite(r.value.data(),r.value.size(),1,fp_) == 0){
        return StatusCode::INVALID_VALUE;
    }
}

}//namespace PUF;