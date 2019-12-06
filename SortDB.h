#ifndef SORT_DB_H
#define SORT_DB_H
#include<map>
#include<thread>
#include<queue>

#include"base.h"
#include"FileIterator.h"
#include"Record.h"
#include"SortedFile.h"
#include"SpinLock.h"
#include"leveldb/port/port_stdcxx.h"

namespace PUF{
struct RecordAndIndex{
    RecordPtr record_ptr_;
    int index_;
    RecordAndIndex(RecordPtr record_ptr, int index):record_ptr_(record_ptr),index_(index){}

    bool operator < (const RecordAndIndex &ri) const {
        return  record_ptr_->get_real_key().compare(ri->record_ptr_->get_real_key());
    }
};

static int kDefaultSortThreadNum = 4;
class SortDB {
public:   
    SortDB(char* filename);
    vector<std::string> get(const std::string &key);
protected:
    void preprocess();
    void readFromUnsortedFile();
    void sortRecords();
    void sortFiles();
    
private:
    leveldb::port::Mutex mu_;
    leveldb::port::CondVar cv_;
    
    FILE* unsorted_fp_;
    SpinLock s_;
    //following variables are protected by s_
    std::map<std::string,SortedFile*> sorting_files_;
    std::vector<SortedFile*> sorted_files_;
    bool wait_sst_write;
    //end
    
    int sort_thread_num = kDefaultSortThreadNum;
   
    std::vector<std::thread> sort_threads_;
    std::thread* read_thread_;

    char db_name_[kKiloByte + 4];
    leveldb::DB *db_;
    std::priority_queue<RecordAndIndex> pq_;
};
}//namespace SORT_DB
#endif