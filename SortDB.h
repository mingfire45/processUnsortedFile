#ifndef SORT_DB_H
#define SORT_DB_H
#include"base.h"
#include"FileIterator.h"
#include"record.h"
#include"SortedFile.h"
#include"SpinLock.h"
#include"leveldb/port/port_stdcxx.h"

#include<map>
#include<thread>

namespace PUF{

static int kDefaultSortThreadNum = 4;
class SortDB {
    SortDB(char* filename);
    void preprocess();
    void readFromUnsortedFile();
    void sortRecords();
private:
    FILE* unsorted_fp_;
    SpinLock s_;
    //following variables are protected by s_
    std::map<std::string,SortedFile*> sorting_files_;
    std::vector<SortedFile*> sorted_files_;
    bool wait_sst_write;
    //end
    
    int sort_thread_num = kDefaultSortThreadNum;
    leveldb::port::Mutex mu_;
    leveldb::port::CondVar cv_;
    std::vector<std::thread> sort_threads_;
    std::thread* read_thread_;
};
}//namespace SORT_DB
#endif