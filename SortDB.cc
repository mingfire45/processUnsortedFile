#include"SortDB.h"
namespace PUF{
static const int kNumReadItems = 100;
static const long kMaxMemoryCanBeUsed = 2 * (long)1024 * 1024 * 1024;
SortDB::SortDB(char *filename):cv_(&mu_),wait_sst_write(false){
    unsorted_fp_ = fopen(filename,"wb");
    if (unsorted_fp_ == nullptr) {
        std::cout<<"open " << filename << " occur error " << std::endl;
        std::cout<<strerror(errno) << std::endl;
        exit(1);
    }      
    preprocess();
}

void SortDB::readFromUnsortedFile(){
    FileIterator unsorted_fi(unsorted_fp_);    
    bool need_read_from_unsorted_file = true;
    auto &record_pool = RecordPool::getInstance();
    while (true) {
        std::vector<RecordPtr> *records = new std::vector<Record*>();
        for (int i = 0 ; i < kNumReadItems ; ++i) {
            RecordPtr r = std::make_shared<Record>(); 
            auto ret_code = unsorted_fi.readRecordFromUnsortedFile(r);
            if (ret_code ==  StatusCode::EOF_OF_FILE){
                if (!records->empty()){
                   record_pool.putRecordsIntoPool(records);
                }                
                has_records_in_unsorted_file.store(true);
                need_read_from_unsorted_file = false;
                break;
            } else if(ret_code == StatusCode::SUCCESS){
                records->push_back(r);
                break;
            } else {
                std::cout<< "read record from unsorted file error " << std::endl;
                exit(1);
            }            
        }        
        if (!need_read_from_unsorted_file) {
            break;
        }
        if (used_memory > kMaxMemoryCanBeUsed) {
            //try to let biggest sst write to disk
            s_.lock();
            assert(!sorting_files_.empty());
            auto max_iter = sorting_files_.begin();
            auto max_size = max_iter->second->totalRecordSize();
            for (auto iter = sorting_files_.begin(); iter != sorting_files_.end(); ++iter) {
                if (iter->second->totalRecordSize() > max_size){
                    max_iter = iter;
                    max_size = iter->second->totalRecordSize();
                }                
            }
            max_iter->second->flagShouldWriteToSST();
            record_pool.putRecordsIntoPool(records,true);
            wait_sst_write = true;
            s_.unLock();
            cv_.Wait();
        } else {
            record_pool.putRecordsIntoPool(records);
        }
    }
}


void SortDB::sortRecords(){
    auto& record_pool = RecordPool::getInstance();
    while (true) {
        auto* sf = new SortedFile();        
        s_.lock();
        sorting_files_[std::string(sf->getFileName())] = sf;
        s_.unLock();
        sf->readFromRecordPool();

        s_.lock();
        sorting_files_.erase(std::string(sf->getFileName()));
        sorted_files_.push_back(sf);
        if (wait_sst_write) {
            cv_.Signal();
            wait_sst_write = false;
        }
        s_.unLock();

        if (!has_records_in_unsorted_file.load() && record_pool.size() == 0) {
            break;
        }
    }
}

void SortDB::preprocess(){
    read_thread_ = new std::thread(&SortDB::readFromUnsortedFile,this);
    for (int i = 0 ; i < kDefaultSortThreadNum ; ++i){
        sort_threads_.push_back(std::thread(&SortDB::sortRecords,this));
    }
    read_thread_->join();
    for (int i = 0 ; i < kDefaultSortThreadNum ; ++i){
        sort_threads_[i].join();
    }        
}

}//namespace puf

