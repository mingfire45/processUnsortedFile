#include"SortDB.h"
namespace PUF{
static const int kNumReadItems = 100;
static const long kMaxMemoryCanBeUsed = 2 * (long)1024 * 1024 * 1024;
SortDB::SortDB(char *filename):cv_(&mu_),wait_sst_write(false){
    unsorted_fp_ = fopen(filename,"wb");
    if (unsorted_fp_ == nullptr) {
        std::cout << "open " << filename << " occur error " << std::endl;
        std::cout << strerror(errno) << std::endl;
        exit(1);
    }
    if (strlen(filename) > kKiloByte){
        std::cerr << " filename is too long " << std::endl;
        exit(1);
    }
    strcpy(db_name_,filename);
    strncpy(db_name + strlen(filename),"_db",4);     
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
            if (ret_code == StatusCode::EOF_OF_FILE){
                if (!records->empty()){
                   record_pool.putRecordsIntoPool(records);
                }                
                has_records_in_unsorted_file.store(false);
                need_read_from_unsorted_file = false;
                break;
            } else if(ret_code == StatusCode::SUCCESS){
                records->push_back(r);                
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

void SortDB::preprocess() {
    read_thread_ = new std::thread(&SortDB::readFromUnsortedFile,this);
    for (int i = 0 ; i < kDefaultSortThreadNum ; ++i){
        sort_threads_.push_back(std::thread(&SortDB::sortRecords,this));
    }
    read_thread_->join();
    for (int i = 0 ; i < kDefaultSortThreadNum ; ++i){
        sort_threads_[i].join();
    }        
}

vector<std::string> SortDB::get(const std::string &key){
    std::string key_processed = key;
    if(key.size() > 20){
        key_processed = sha1(key);
    }
    std::string value;
    db_.Get((leveldb::ReadOptions(), key, &value);
    vector<std::string> values;
    int curr = 0;
    char value_type;
    uint16_t value_size;
    while(true){        
        char short_value[50];
        memcpy(value_type,value + curr,1);
        if(value_type == SHORT_VALUE){
            memcpy(value_size,value + curr + 1, 2);
            memcpy(short_value,value + curr + 3, value_size);
            values.emplace_back(std::string(short_value,value_size));
        }else{
            //TODO: read from file
        }
    }
    return values;
}

void SortDB::sortFiles() {    
    leveldb::Options options;  
    options.create_if_missing = true;  
    leveldb::Status status = leveldb::DB::Open(options, db_name_, &db_);  
    assert(status.ok());  

    std::vector<RecordPtr> records;
    std::vector<char*> keys;
    std::vector<char*> values;

    for (auto file: sorted_files_) {
        file->reOpenForRead();
        records.push_back(std::make_shared<Record>());
        keys.push_back(new char[30]);
        values.push_back(new char[1024]);
    }

    for (int i = 0 ; i < sorted_files_.size(); ++i) {
        file->getFi().readRecordFromSortedFile(records[i], keys[i], values[i]);
        pq_.push(RecordAndIndex(records[i],i));
    }

    while (true) {
        auto top = pq_.top();
        vector<int> indexs;
        indexs.push(top.index_);
        pq_.pop();
        
        std::string key(top.record_ptr_->key.data(),top.record_ptr_->key.size());
        std::string value(top.record_ptr->value.data(),top.record_ptr->value.size());
        while(!pq.empty()){
            if (top.record_ptr_->key.compare(pq_.top().record_ptr_->key) == 0) {
                value.append(pq_.top().record_ptr->value.data(),record_ptr->value.size());
                index.push(top.index_);
                pq_.pop();
            }
        }

        for(int i = 0 ; i < indexs.size() ; i++){
            if(sorted_files_[t].readRecordFromSortedFile(records[indexs[i]],keys[indexs[i]],values[indexs[i]]) == StatusCode::SUCCESS){
                pq_.push(RecordAndIndex(records[i],indexs[i]));
            }
        }

        db_.Put(leveldb::WriteOptions(), key, value);
    }


}


}//namespace puf

