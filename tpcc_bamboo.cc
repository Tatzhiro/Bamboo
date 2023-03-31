#include <ctype.h>
#include <pthread.h>
#include <sched.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <cctype>

#include "boost/filesystem.hpp"

#define GLOBAL_VALUE_DEFINE

#include "include/common.hh"
#include "include/result.hh"
#include "include/transaction.hh"
#include "include/util.hh"

#include "../include/atomic_wrapper.hh"
#include "../include/backoff.hh"
#include "../include/cpu.hh"
#include "../include/debug.hh"
#include "../include/fileio.hh"
#include "../include/masstree_wrapper.hh"
#include "../include/random.hh"
#include "../include/result.hh"
#include "../include/tpcc.hh"
#include "../include/tsc.hh"
#include "../include/util.hh"
#include "../include/zipf.hh"

using namespace std;

void Tuple::ownersAdd(int txn)
{
  owners.emplace_back(txn);
}

void worker(size_t thid, char &ready, const bool &start, const bool &quit) {
  Result &myres = std::ref(SS2PLResult[thid]);
  TxExecutor trans(thid, (Result *)&myres, quit);
  TPCCWorkload<Tuple,void> workload;
  workload.prepare(trans, nullptr);
  Backoff backoff(FLAGS_clocks_per_us);

  TxPointers[thid] = &trans;

#if NONTS == 1
  thread_timestamp[thid] = thid;
#endif


#ifdef Linux
  setThreadAffinity(thid);
  // printf("Thread #%d: on CPU %d\n", res.thid_, sched_getcpu());
  // printf("sysconf(_SC_NPROCESSORS_CONF) %d\n",
  // sysconf(_SC_NPROCESSORS_CONF));
#endif

#if MASSTREE_USE
  MasstreeWrapper<Tuple>::thread_init(int(thid));
#endif

  storeRelease(ready, 1);
  while (!loadAcquire(start)) _mm_pause();
  while (!loadAcquire(quit)) {
    if (thid == 0) leaderBackoffWork(backoff, SS2PLResult);
    workload.run<TxExecutor,TransactionStatus>(trans);
  }

  return;
}

int main(int argc, char *argv[])
try
{
  gflags::SetUsageMessage("Bamboo benchmark.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  chkArg();
  ShowOptParameters();
  // FLAGS_tpcc_num_wh = FLAGS_thread_num;
  TPCCWorkload<Tuple,void>::displayWorkloadParameter();
  TPCCWorkload<Tuple,void>::makeDB(nullptr);
  for (int i = 0; i < FLAGS_thread_num; i++)
  {
    thread_stats[i] = 0;
    thread_timestamp[i] = 0;
    commit_semaphore[i] = 0;
  }
  alignas(CACHE_LINE_SIZE) bool start = false;
  alignas(CACHE_LINE_SIZE) bool quit = false;
  initResult();
  std::vector<char> readys(FLAGS_thread_num);
  std::vector<std::thread> thv;
  for (size_t i = 0; i < FLAGS_thread_num; ++i)
    thv.emplace_back(worker, i, std::ref(readys[i]), std::ref(start),
                     std::ref(quit));
  waitForReady(readys);
  // printf("Press any key to start\n");
  // int c = getchar();
  // cout << "start" << endl;
  storeRelease(start, true);
  for (size_t i = 0; i < FLAGS_extime; ++i)
  {
    sleepMs(1000);
  }
  storeRelease(quit, true);
  for (auto &th : thv)
    th.join();

  for (unsigned int i = 0; i < FLAGS_thread_num; ++i)
  {
    SS2PLResult[0].addLocalAllResult(SS2PLResult[i]);
  }
  ShowOptParameters();
  SS2PLResult[0].displayAllResult(FLAGS_clocks_per_us, FLAGS_extime, FLAGS_thread_num);
  // cout << "first thread commit:\t" << SS2PLResult[0].local_commit_counts_ << endl;
  // cout << "last thread commit:\t" << SS2PLResult[FLAGS_thread_num - 1].local_commit_counts_ << endl;
  // for (unsigned int i = 0; i < FLAGS_thread_num; ++i)
  // {
  //   printf("%d,", SS2PLResult[i].local_commit_counts_);
  // }
  return 0;
}
catch (bad_alloc)
{
  printf("bad alloc\n");
  ERR;
}