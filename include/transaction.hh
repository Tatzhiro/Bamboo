#pragma once

#include <vector>

#include "../../include/procedure.hh"
#include "../../include/backoff.hh"
#include "../../include/result.hh"
#include "../../include/rwlock.hh"
#include "../../include/string.hh"
#include "../../include/util.hh"
#include "../../include/status.hh"
#include "../../include/workload.hh"
#include "ss2pl_op_element.hh"
#include "tuple.hh"

enum class TransactionStatus : uint8_t {
  inFlight,
  committed,
  aborted,
};

extern void writeValGenerator(char *writeVal, size_t val_size, size_t thid);

struct myVector {
  int8_t size;
#if FAIR
  int arr[224];
#else
  int arr[224];
#endif
  myVector() {
    size = 0;
    for (int i = 0; i < 224; i++) arr[i] = 0;
  }
};

class TxExecutor {
public:
  alignas(CACHE_LINE_SIZE) int thid_;
  int txid_;
  const bool& quit_; // for thread termination control
  TransactionStatus status_ = TransactionStatus::inFlight;
#if RANDOM == 1
  Xoroshiro128Plus rnd;
#endif
  Result *result_;
  vector <ReadElement<Tuple>> read_set_;
  vector <WriteElement<Tuple>> write_set_;
  vector <Procedure> pro_set_;
  myVector all_owners;
  // char write_val_[VAL_SIZE];
  // char return_val_[VAL_SIZE];

  TxExecutor(int thid, Result *sres, const bool &quit) : thid_(thid), result_(sres), txid_(thid), quit_(quit) {
    read_set_.reserve(FLAGS_max_ope);
    write_set_.reserve(FLAGS_max_ope);
    pro_set_.reserve(FLAGS_max_ope);
#if RANDOM == 1
    rnd.init();
#endif
    // genStringRepeatedNumber(write_val_, VAL_SIZE, thid);
    // genStringRepeatedNumber(return_val_, VAL_SIZE, thid);
  }

  ReadElement<Tuple> *searchReadSet(Storage s, std::string_view key);

  WriteElement<Tuple> *searchWriteSet(Storage s, std::string_view key);

  void warmupTuple(std::string_view key);

  void begin();

  Status read(Storage s, std::string_view key, TupleBody** body);

  Status write(Storage s, std::string_view key, TupleBody&& body);

  void readWrite(uint64_t key, bool should_retire);

  bool commit();

  void abort();

  void unlockList(bool is_abort);

  // inline
  Tuple *get_tuple(Tuple *table, uint64_t key) { return &table[key]; }

  bool conflict(LockType x, LockType y);

  void checkWound(vector<int> &list, LockType lock_type, Tuple *tuple, std::string_view key);

  void PromoteWaiters(Tuple *tuple);

  void writelockAcquire(LockType lock_type, std::string_view key, Tuple *tuple);

  bool LockRelease(bool is_abort, std::string_view key, Tuple *tuple);

  void LockRetire(std::string_view key, Tuple *tuple);

  bool spinWait(std::string_view key, Tuple *tuple, Storage s);

  bool lockUpgrade(std::string_view key, Tuple *tuple);

  void checkLists(std::string_view key, Tuple *tuple);

  void eraseFromLists(Tuple *tuple); // erase txn from waiters and owners lists in case of abort during spinwait

  vector<int>::iterator woundRelease(int txn, Tuple *tuple, std::string_view key);
  
  void cascadeAbort(int txn, Tuple *tuple, std::string_view key);

  void concat(vector<int> &r, vector<int> &o);

  void addCommitSemaphore(int t, LockType t_type, Tuple *tuple);

  bool readlockAcquire(LockType lock_type, std::string_view key, Tuple *tuple, Storage s);

  bool readWait(Tuple *tuple, std::string_view key);
  
  bool adjustFollowingSemaphore(Tuple *tuple, int txn);

  void leaderWork(){};

  bool isLeader(){
    return thid_ == 0;
  }

};
