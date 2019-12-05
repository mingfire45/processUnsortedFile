#ifndef SORT_FILE_H
#define SORT_FILE_H
#include<iostream>
#include<queue>
#include"base.h"
#include<atomic>
#include"RecordPool.h"
#include"FileIterator.h"

namespace PUF{
static std::atomic<int> FILE_ID(0);
//1GB
static size_t kMAX_SORTED_FILE_SIZE = kGigaByte;
struct RecordPtrComp{
    bool operator()(const RecordPtr &r1, cnst RecordPtr &r2) const{
        return r1->get_real_key().compare(r2->get_real_key());
    }
};
class SortedFile{
public:
    SortedFile():total_record_size_(0),should_write_to_sst_(false){
        sprintf(file_name_,"Sorted%d.sst",FILE_ID.load());
        ++FILE_ID;
        FILE *fp = fopen(file_name_,"wb");
        if(fp == nullptr){
            std::cout<<"can't create new sorted file"<<std::endl;
            std::cout<<strerror(errno)<<std::endl;
            exit(errno);
        }
        fi.setFile(fp);
    }
    void readFromRecordPool();

    //no need to be accurte
    size_t totalRecordSize() const {
        return total_record_size_;
    }

    const char* getFileName() const{
        return file_name_;
    }

    bool operator < (const SortedFile &sf) const {
        return strcmp(getFileName(),this->getFileName());
    }

    void writeToSSTFile();

    void flagShouldWriteToSST(){
        should_write_to_sst_.store(!should_write_to_sst_.load());
    }

private:
    std::priority_queue<RecordPtr,RecordPtrComp>  pq_;
    size_t total_record_size_;
    std::atomic<bool> should_write_to_sst_;
    char file_name_[30];
    FileIterator fi;
};

class SortedFilePtr{
public:    
    explicit SortedFilePtr(SortedFile* sf):sf_(sf){}
    
    bool operator< (const SortedFilePtr& sfp) const{
        return (*(this->sf_)) < (*sfp.getSortedFile());
    }
    SortedFile* getSortedFile() const{
        return sf_;
    }
private:    
    SortedFile* sf_
};

}// namespace PUF

#endif