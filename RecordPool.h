#ifndef RECORD_POOL_H
#define RECORD_POOL_H
#include<iostream>
#include<queue>
#include<mutex>
#include<vector>

#include"leveldb/port/port_stdcxx.h"
#include"Record.h"
#include"base.h"

namespace PUF{

class RecordPool{    
public:
    void putRecordsIntoPool(std::vector<RecordPtr>* records,bool signAll = false);

    std::vector<RecordPtr>* getRecordsFromPool();        
    static RecordPool &getInstance(){
        static RecordPool record_pool;
        return record_pool;
    }

    RecordPool(const RecordPool&) = delete;
    RecordPool& operator=(const RecordPool&) = delete;


    size_t size(){
        mu_.Lock();
        size_t ret = record_pools_.size();
        mu_.Unlock();
        return ret;
    }
protected:
    RecordPool():cv_(&mu_){}
private:        
    leveldb::port::Mutex mu_;
    leveldb::port::CondVar cv_;
    std::queue<std::vector<RecordPtr>*> record_pools_;            
};
}// namespace PUF

#endif