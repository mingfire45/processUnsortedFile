# 思路
  因为日常加班，所以使用了一种比较简单的思路。key的范围较广，从1B到1KB，于是首先把key的大小范围做一个限定，对于小于20B 的key 不做处理，对于大于20B的，进行SHA1 计算，生成新的key，因为2的160次方种组合已经能带来较低的hash 冲突率，当然对于hash 冲突下面也描述有解决方案。
  value的范围也很广，从1B到1MB，于是对于key-value 大小小于50B 的不做处理，对于大于50B 的做key-value 分离处理。

# 具体实现方式
  将所有记录从未排序的文件中以顺序读地方式读出，每读100条记录，就放入到一个内存块中。这些记录读取出来时，对key 进行处理，大于20B进行SHA1 计算，对于key-value大小超过50B的，在value里面装key-value对在原无序文件中的偏移。因此需要对key-value进行如下编码：
  ```C++
    // key_size(uint16_t) key(char *)
    leveldb::Slice key;
    // value is divided into two types
    // type 0 value_size(uint16_t) value(char *)
    // type 1 len_of_record(uint16_t) value_loc(uint64_t)
    leveldb::Slice value;
   // 这里用到了leveldb 的slice， slice 是个轻量级的string, 维护执行sr的指针和str的长度。   
   
  ``` 
   
   另外起若干个线程，每个线程内维护一个优先队列，从缓存块中读数据，加入到自己的优先队列中按key进行排序。当所有优先队列占用的内存大小超过限定大小（这里设置为2GB）时，挑选出其中一个最大的，开始向外存写文件，这次写的是有序的key-value对。或者当优先队列本身的大小超过1GB时，也开始向外存写文件。这样外存里面最终会积累若干个有序的文件。
   
   最后，每个有序文件一个迭代器，将这些迭代器按key 排序，每次选择最小的key，塞入到leveldb 的db库中。若本次塞的key与之前的key相等，则做一个合并动作，把本次的key-value对和入到之前的key对应的value中，以这种方式来处理key重复或者经过SHA1计算后hash冲突的key-value对。
   
   用户通过key 来leveldb 的db 库中取key-value对，首先根据key的大小来判断是否要经行SHA1 计算。 然后再根据取出的key-value对编码，判断是否要去无序文件中拿取key-value对。
   
   

