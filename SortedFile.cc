#include"SortedFile.h"
#include<cstdio>
namespace PUF{

void SortedFile::readFromRecordPool() {
    auto &record_pool = RecordPool::getInstance();
    while (true) {
        //may wait
        std::vector<RecordPtr> *records = record_pool.getRecordsFromPool();
        if (records == nullptr) {
            if (!has_records_in_unsorted_file.load()) {
                writeToSSTFile();
                return;
            }            
        } else {
            for (int i = 0 ; i < records->size() ; ++i) {
                pq_.push(records->at(i));
                total_record_size_ += (records->at(i)->size());
            }
            delete records;
        }   
        if (total_record_size_ >= kMAX_SORTED_FILE_SIZE || should_write_to_sst_){
            writeToSSTFile();
            break;
        }
    }    
}

void SortedFile::writeToSSTFile(){
    while(!pq_.empty()){
        RecordPtr r = pq_.top();
        fi.writeToSortedFile(*r);        
        delete r;
        pq_.pop();
    }
    fi.flush();    
}


} // namespace PUF
