#ifndef FILE_ITERATOR_H
#define FILE_ITERATOR_H
#include"base.h"
#include"Record.h"
#include <unistd.h>
namespace PUF{
class FileIterator{
public:
    FileIterator(FILE *fp):fp_(fp),curr_(0){                
        //set buffer
    }
    FileIterator():fp_(nullptr),curr_(0){}
    // 0 means success
    // 1 means EOF
    StatusCode readRecordFromUnsortedFile (RecordPtr &r);

    StatusCode readRecordFromSortedFile (RecordPtr &r);

    StatusCode writeToUnSortedFile(const Record &r);

    StatusCode writeToSortedFile(const Record &r);

    void setFile(FILE *fp){
        fp_ = fp;        
    }

    void flush(){
        fflush(fp_);
        fsync(fileno(fp_));
    }
    
private:
    FILE *fp_;
    off_t curr_;    
    uint64_t key_size_;
    uint64_t value_size_;
    char value_buf_[kMegaByte];
    char key_buf_[kKiloByte];
};

}//namespace PUF

#endif