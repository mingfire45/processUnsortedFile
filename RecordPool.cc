#include"RecordPool.h"
namespace PUF{

void RecordPool::putRecordsIntoPool(std::vector<Record*>* records, bool signAll){
    mu_.Lock();
    record_pools_.push(records);        
    if (signAll) {
        cv_.SignalAll();
    } else if (record_pools_.size() == 1) {
        cv_.Signal();
    } 
    mu_.Unlock();
}

std::vector<Record*>* RecordPool::getRecordsFromPool(){
    mu_.Lock();
    std::vector<Record*> *ret = nullptr;
    if (!record_pools_.empty()) {
        ret = record_pools_.front();   
        record_pools_.pop();    
        if (record_pools_.empty() && !has_records_in_unsorted_file.load()){
            cv_.SignalAll();
        }
    } else if (has_records_in_unsorted_file.load()) {                            
        cv_.Wait();
    }
    mu_.Unlock();
    return ret;        
}

}//namespace PUF

