#include<cstdio>
#include<include/leveldb/slice.h>
#include<iostream>
#include "base.h"

int main(){
    
    uint64_t key_size,value_size;
    char key1[] = "123";
    uint64_t key1_size = sizeof(key1);
    char value1[] = "4567";
    uint64_t value1_size = sizeof(value1);
    char key2[] = "678";
    uint64_t key2_size = sizeof(key2);
    std::string value2(1025,0);
    value2[0] = 1;
    uint64_t value2_size = 1025; 
        
    FILE *fp = fopen("tempfile.txt","wb");
    PUF::FileIterator fi(fp);
    leveldb::Slice slice_key1(key1,key1_size);
    leveldb::Slice slice_value1(value1,value1_size);

    leveldb::Slice slice_key2(key2,key2_size);
    leveldb::Slice slice_value2(value2.c_str(),value2_size);

    PUF::Record r1(slice_key1,slice_value1);
    PUF::Record r2(slice_key2,slice_value2);
    fi.WriteToUnSortedFile(r1);
    fi.WriteToUnSortedFile(r2);
    fclose(fp);

    FILE *rfp = fopen("tempfile.txt","rb");
    PUF::FileIterator fr(rfp);
    r1.setFile(rfp);
    r2.setFile(rfp);

    fr.readRecordFromFile(&r1);
    fr.readRecordFromFile(&r2);        
    leveldb::Slice real_key = r1.get_real_key();
    leveldb::Slice real_value = r2.get_real_value();

    for(int i = 0 ; i < real_key.size() ; i++){
        std::cout << real_key.data()[i];
    }
    std::cout<<std::endl;

    real_value = r2.get_real_value();
    for(int i = 0 ; i < real_value.size(); i++){
        std::cout << real_value.data()[i];
    }
    std::cout<<std::endl;
}