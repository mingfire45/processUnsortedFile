#include<cstdio>
#include<include/leveldb/slice.h>
#include<iostream>
struct record {
    char *key;
    char *value;
    uint64_t key_size;
    uint64_t value_size;
}
enum class StatusCode{
    SUCCESS = 0,
    INVALID_KEY_SIZE,
    INVALID_KEY,
    INVALID_VALUE_SIZE,
    INVALID_VALUE
};

class FileIterator{
public:    
    File *fp;
    StatusCode WriteRecord(const record &r){
        if(fwrite(&r.key_size,8,1,fp) == 0){            
            return INVALID_KEY_SIZE;
        }
        if(fwrite(r.key,r.key_size,1,fp) == 0){
            return INVALID_KEY;
        }
        if(fwrite(&r.value_size,8,1,fp) == 0){
            return INVALID_VALUE_SIZE;
        }
        if(fwrite(r.value,r.value_size,1,fp) == 0){
            return INVALID_VALUE;
        }
    }

    StatusCode ReadRecord(record *r){
        if (fread(&r.key_size,8,1,fp) == 0) {            
            return INVALID_KEY_SIZE;
        }
        if (fread(r.key,r.key_size,1,fp) == 0) {
            return INVALID_KEY;
        }
        if (fread(&r.value_size,8,1,fp) == 0) {
            return INVALID_VALUE_SIZE;
        }
        if (fread(r.value,r.value_size,1,fp) == 0) {
            return INVALID_VALUE;
        }
    }
}
int main(){
    
    uint64_t key_size,value_size;
    char key1[] = "123";
    uint64_t key1_size = sizeof(key1);
    char value1[] = "4567";
    uint64_t value1_size = sizeof(value1);
    char key2[] = "678";
    uint64_t key2_size = sizeof(key2);
    char value2[] = '9101112';
    uint64_t value2_size = sizeof(value2);

    record r1 = {key1,value1,key1_size,value1_size};
    record r2 = {key2,value2,key2_size,value2_size};
    
    FILE *fp = fopen("tempfile.txt","w");
    FileIterator fi;
    fi.fp = fp;
    fi.WriteRecord(r1);
    fi.WriteRecord(r2);

    fclose(fp);

    FILE *rfp = fopen("tempfile.txt","r");
    FileIterator fr;
    fr.fp = rfp;
    fr.ReadRecord(&r1);
    fr.ReadRecord(&r2);

    std::cout<< r1.key << r1.value << std::endl;
    std::cout<< r2.key << r2.value << std::endl;
    
    std::cout<<fr.ReadRecord() << std::endl;
    std::cout<<feof(rfp)  << std::endl;

}