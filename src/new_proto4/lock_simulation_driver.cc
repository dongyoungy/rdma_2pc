#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <limits.h>
#include <pthread.h>
#include <sys/times.h>
#include <cmath>
#include <iostream>
#include <numeric>
#include <vector>

#include "Poco/NumberParser.h"
#include "Poco/StringTokenizer.h"
#include "Poco/Thread.h"

#include "d2lm_lock_client.h"
#include "hotspot_exclusive_lock_simulator.h"
#include "hotspot_lock_simulator.h"
#include "lock_manager.h"
#include "lock_simulator.h"
#include "mpi.h"
#include "powerlaw_lock_simulator.h"
#include "test_lock_simulator.h"
#include "tpcc_lock_simulator.h"

using namespace std;
using namespace rdma::proto;

void* MeasureCPUUsage(void* args);

struct CPUUsage {
  double total_cpu;
  double num_sample;
  bool terminate;
};

int main(int argc, char** argv) {
  // MPI_Init(&argc, &argv);
  MPI_Init(&argc, &argv);

  if (argc != 12) {
    cout << "USAGE: " << argv[0] << " <work_dir> <num_lock_object> <duration>"
         << " <request_size> <num_users> <lock_mode>"
         << " <num_retry>"
         << " <workload_type> <think_time_type> <think_time_duration> "
            "<enable_random_backoff> "
         << endl;
    exit(1);
  }

  int num_managers, rank;

  MPI_Comm_size(MPI_COMM_WORLD, &num_managers);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  if (rank == 0) {
    if (1 == htons(1)) {
      cout << "The current machine uses BIG ENDIAN" << endl;
    } else {
      cout << "The current machine uses LITTLE ENDIAN" << endl;
    }
  }

  int k = 1;
  string work_dir = argv[k++];
  int num_lock_object = atoi(argv[k++]);
  int duration = atoi(argv[k++]);
  int request_size = atoi(argv[k++]);
  int num_users = atoi(argv[k++]);
  string lock_mode_str = argv[k++];
  int num_retry = atoi(argv[k++]);
  string workload_type = argv[k++];
  string think_time_type = argv[k++];
  int think_time_duration = atoi(argv[k++]);
  string random_backoff_str = argv[k++];

  int d2lm_deadlock_limit = 0;
  bool d2lm_do_read_backoff = false;
  LockMode lock_mode = PROXY_RETRY;
  // if (lock_mode_str == "proxy-retry") {
  // lock_mode = PROXY_RETRY;
  //} else if (lock_mode_str == "proxy-queue") {
  // lock_mode = PROXY_QUEUE;
  //} else if (lock_mode_str == "remote-poll") {
  // lock_mode = REMOTE_POLL;
  //} else if (lock_mode_str == "remote-notify") {
  // lock_mode = REMOTE_NOTIFY;
  //} else if (lock_mode_str == "remote-queue") {
  // lock_mode = REMOTE_QUEUE;
  //} else {
  // cerr << "Invalid Lock Mode: " << lock_mode_str << endl;
  // exit(-1);
  //}

  int tpcc_dist2_num_server = num_managers / 5;
  int tpcc_dist2_num_client = tpcc_dist2_num_server * 4;
  if (lock_mode_str == "traditional") {
    lock_mode = PROXY_QUEUE;
  } else if (lock_mode_str == "simple_retry") {
    lock_mode = REMOTE_POLL;
    LockManager::SetSharedExclusiveRule("fail");
    LockManager::SetExclusiveSharedRule("fail");
    LockManager::SetExclusiveExclusiveRule("fail");
  } else if (lock_mode_str == "drtm") {
    lock_mode = REMOTE_DRTM;
    LockManager::SetSharedExclusiveRule("fail");
    LockManager::SetExclusiveSharedRule("poll");
    LockManager::SetExclusiveExclusiveRule("fail");
  } else if (lock_mode_str == "ncosed") {
    lock_mode = REMOTE_NOTIFY;
  } else if (strncasecmp(lock_mode_str.c_str(), "d2lm", 4) == 0) {
    lock_mode = REMOTE_D2LM_V2;
    Poco::StringTokenizer tokenizer(lock_mode_str, "_",
                                    Poco::StringTokenizer::TOK_TRIM);
    if (tokenizer.count() < 2) {
      cerr << "Incorrect number of tokens for D2LM." << endl;
      exit(ERROR_INVALID_LOCK_MODE);
    }
    d2lm_deadlock_limit = Poco::NumberParser::parse(tokenizer[1]);
    d2lm_do_read_backoff = (tokenizer.count() == 3) ? true : false;
  } else {
    cerr << "Invalid lock mode: " << lock_mode_str << endl;
    exit(ERROR_INVALID_LOCK_MODE);
  }

  bool do_random_backoff = false;
  if (random_backoff_str == "true") {
    do_random_backoff = true;
  } else if (random_backoff_str == "false") {
    do_random_backoff = false;
  } else {
    cerr << "Invalid random backoff option: " << random_backoff_str << endl;
    exit(ERROR_INVALID_RANDOM_BACKOFF);
  }

  if (strncasecmp(workload_type.c_str(), "tpcc", 4) == 0) {
    num_lock_object = kTPCCNumObjects * kTPCCMaxWarehouse;
  }

  if (rank == 0) {
    cout << "# of Nodes = " << num_managers << endl;
    cout << "# Users per node = " << num_users << endl;
    cout << "Type of Workload = " << workload_type << endl;
    cout << "Type of Think Time = " << think_time_type << endl;
    cout << "Duration = " << duration << " s" << endl;
    cout << "# Requests per Tx = " << request_size << endl;
    cout << "# Lock Objects = " << num_lock_object << endl;
    cout << "Lock Mode = " << lock_mode_str << endl;
    cout << "D2LM Deadlock Limit = " << d2lm_deadlock_limit << endl;
    cout << "D2LM Perform Read Backoff = "
         << (d2lm_do_read_backoff ? "TRUE" : "FALSE") << endl;
  }

  LockManager::SetFailRetry(num_retry);
  LockManager::SetPollRetry(num_retry);
  D2LMLockClient::SetDeadLockLimit(d2lm_deadlock_limit);
  D2LMLockClient::SetReadBackoff(d2lm_do_read_backoff);
  LockManager* lock_manager =
      new LockManager(argv[1], rank, num_managers, num_lock_object, lock_mode);

  LockManager* temp_manager = lock_manager;
  if (lock_manager->Initialize()) {
    cerr << "LockManager initialization failure." << endl;
    exit(-1);
  }

  Poco::Thread manager_thread;
  manager_thread.start(*lock_manager);

  // std::vector<std::unique_ptr<LockSimulator>> users;
  std::vector<LockSimulator*> users;
  LockSimulator* temp_user;
  for (int i = 0; i < num_users; ++i) {
    int id = (rank * num_users) + (i + 1);
    // std::unique_ptr<LockSimulator> simulator;
    LockSimulator* simulator;
    // TODO: Add other simulators.
    if (workload_type == "simple") {
      simulator =
          new LockSimulator(lock_manager, id, num_managers, num_lock_object,
                            request_size, think_time_type, do_random_backoff);
    } else if (workload_type == "test") {
      simulator =
          new TestLockSimulator(lock_manager, id, num_managers, num_lock_object,
                                think_time_type, do_random_backoff);
    } else if (workload_type == "hotspot") {
      simulator = new HotspotLockSimulator(lock_manager, id, num_managers,
                                           num_lock_object, request_size,
                                           think_time_type, do_random_backoff);
    } else if (strncasecmp(workload_type.c_str(), "powerlaw", 8) == 0) {
      Poco::StringTokenizer tokenizer(workload_type, "_",
                                      Poco::StringTokenizer::TOK_TRIM);
      if (tokenizer.count() < 3) {
        cerr << "Incorrect number of tokens for Powerlaw simulator." << endl;
        exit(ERROR_INVALID_LOCK_MODE);
      }
      double exponent = Poco::NumberParser::parseFloat(tokenizer[1]);
      double shared_lock_ratio = Poco::NumberParser::parseFloat(tokenizer[2]);

      simulator = new PowerlawLockSimulator(
          lock_manager, id, num_managers, num_lock_object, request_size,
          think_time_type, do_random_backoff, exponent, shared_lock_ratio);
    } else if (workload_type == "hotspot-exclusive") {
      simulator = new HotspotExclusiveLockSimulator(
          lock_manager, id, num_managers, num_lock_object, request_size,
          think_time_type, do_random_backoff);
    } else if (workload_type == "tpcc-uniform") {
      simulator = new TPCCLockSimulator(
          lock_manager, id, num_managers, num_lock_object, think_time_type,
          do_random_backoff, id % num_managers, false);
    } else if (workload_type == "tpcc-dist") {
      simulator = new TPCCLockSimulator(lock_manager, id, 1, num_lock_object,
                                        think_time_type, do_random_backoff,
                                        id % (num_managers / 2), false);
    } else if (workload_type == "tpcc-dist2") {
      simulator = new TPCCLockSimulator(lock_manager, id, tpcc_dist2_num_server,
                                        num_lock_object, think_time_type,
                                        do_random_backoff,
                                        id % tpcc_dist2_num_server, true, 10);
    } else if (strncasecmp(workload_type.c_str(), "tpcc-hotspot", 12) == 0) {
      Poco::StringTokenizer tokenizer(workload_type, "_",
                                      Poco::StringTokenizer::TOK_TRIM);
      if (tokenizer.count() > 2) {
        cerr << "Incorrect number of tokens for TPCC simulator." << endl;
        exit(ERROR_INVALID_LOCK_MODE);
      }
      int num_warehouse = 1;
      if (tokenizer.count() == 2) {
        num_warehouse = Poco::NumberParser::parse(tokenizer[1]);
      }
      simulator = new TPCCLockSimulator(lock_manager, id, 1, num_lock_object,
                                        think_time_type, do_random_backoff, 0,
                                        false, num_warehouse);
    } else if (workload_type == "tpcc-failover") {
      simulator = new TPCCLockSimulator(
          lock_manager, id, 1, num_lock_object, think_time_type,
          do_random_backoff, (rank - (num_managers / 2)) % (num_managers / 4),
          false);
    } else if (strncasecmp(workload_type.c_str(), "tpcc-random", 11) == 0) {
      Poco::StringTokenizer tokenizer(workload_type, "_",
                                      Poco::StringTokenizer::TOK_TRIM);
      if (tokenizer.count() > 2) {
        cerr << "Incorrect number of tokens for TPCC simulator." << endl;
        exit(ERROR_INVALID_LOCK_MODE);
      }
      int num_warehouse = 1;
      if (tokenizer.count() == 2) {
        num_warehouse = Poco::NumberParser::parse(tokenizer[1]);
      }
      simulator = new TPCCLockSimulator(
          lock_manager, id, (num_managers / 2), num_lock_object,
          think_time_type, do_random_backoff, id % (num_managers / 2), true,
          num_warehouse);
    } else {
      cerr << "Unknown workload: " << workload_type << endl;
      exit(-2);
    }
    simulator->SetThinkTimeDuration(think_time_duration);
    lock_manager->RegisterUser(i + 1, simulator);
    temp_user = simulator;
    // users.push_back(std::move(simulator));
    users.push_back(simulator);
  }

  if (workload_type == "tpcc-failover") {
    // back up node
    if (rank >= num_managers / 4 && rank < num_managers / 2) {
      lock_manager->SetBackupNodeFor(rank - (num_managers / 4) + 1);
    }
  }
  sleep(3);

  if (lock_manager->InitializeLockClients()) {
    cerr << "InitializeLockClients() failed." << endl;
    exit(-1);
  }

  // wait till all clients are initialized
  while (!lock_manager->IsClientsInitialized()) {
    Poco::Thread::sleep(250);
  }

  // measure cpu usage
  pthread_t cpu_measure_thread;
  CPUUsage usage;
  if (pthread_create(&cpu_measure_thread, NULL, &MeasureCPUUsage,
                     (void*)&usage)) {
    cerr << "pthread_create() error." << endl;
    exit(-1);
  }

  uint64_t* current_counts = NULL;
  uint64_t current_count = 0;
  uint64_t current_total_count = 0;
  uint64_t last_total_count = 0;
  uint64_t max_throughput = 0;
  std::vector<uint64_t> throughputs;
  if (rank == 0) {
    current_counts = new uint64_t[num_managers];
    throughputs.clear();
    throughputs.reserve(duration);
  }

  MPI_Barrier(MPI_COMM_WORLD);

  // Start lock simulators
  std::vector<Poco::Thread*> user_threads;
  if (workload_type == "hotspot" || workload_type == "hotspot-exclusive" ||
      workload_type == "tpcc-hotspot") {
    if (rank != 0) {
      for (int i = 0; i < num_users; ++i) {
        Poco::Thread* user_thread = new Poco::Thread;
        user_thread->start(*users[i]);
        user_threads.push_back(user_thread);
      }
    }
  } else if (workload_type == "tpcc-dist") {
    if (rank >= num_managers / 2) {
      for (int i = 0; i < num_users; ++i) {
        Poco::Thread* user_thread = new Poco::Thread;
        user_thread->start(*users[i]);
        user_threads.push_back(user_thread);
      }
    }
  } else if (workload_type == "tpcc-dist2") {
    if (rank >= tpcc_dist2_num_server) {
      for (int i = 0; i < num_users; ++i) {
        Poco::Thread* user_thread = new Poco::Thread;
        user_thread->start(*users[i]);
        user_threads.push_back(user_thread);
      }
    }
  } else if (workload_type == "tpcc-failover") {
    if (rank >= num_managers / 2) {
      for (int i = 0; i < num_users; ++i) {
        Poco::Thread* user_thread = new Poco::Thread;
        user_thread->start(*users[i]);
        user_threads.push_back(user_thread);
      }
    }
  } else if (strncasecmp(workload_type.c_str(), "tpcc-random", 11) == 0) {
    if (rank >= num_managers / 2) {
      for (int i = 0; i < num_users; ++i) {
        Poco::Thread* user_thread = new Poco::Thread;
        user_thread->start(*users[i]);
        user_threads.push_back(user_thread);
      }
    }
  } else {
    for (int i = 0; i < num_users; ++i) {
      Poco::Thread* user_thread = new Poco::Thread;
      user_thread->start(*users[i]);
      user_threads.push_back(user_thread);
    }
  }

  int zero_count = 0;
  for (int i = 0; i < duration; ++i) {
    current_count = 0;
    for (int j = 0; j < num_users; ++j) {
      current_count += users[j]->GetCount();
    }
#ifdef DEBUG
    cout << rank << " : " << current_count << endl;
    std::flush(cout);
#endif
    MPI_Gather(&current_count, 1, MPI_LONG_LONG_INT, current_counts, 1,
               MPI_LONG_LONG_INT, 0, MPI_COMM_WORLD);

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {
      current_total_count = 0;
      for (int j = 0; j < num_managers; ++j) {
        current_total_count += current_counts[j];
      }
      uint64_t current_throughput = current_total_count - last_total_count;
      cout << i << "," << current_throughput << endl;

      throughputs.push_back(current_throughput);
      if (current_throughput > max_throughput) {
        max_throughput = current_throughput;
      }
      last_total_count = current_total_count;
      if (current_throughput == 0) {
        ++zero_count;
      } else {
        zero_count = 0;
      }
#ifdef DEBUG
      if (zero_count >= 5) {
        // cout << "zero throughput: " << getpid() << endl;
        // sleep(600000);
        // abort();
      }
#endif
    }
    // Assume 32 nodes
    if (i == 15 && rank == 0 && workload_type == "tpcc-failover") {
      cout << i << ",Failing LM " << rank << endl;
      lock_manager->DisableRemoteAtomicAccess();
      lock_manager->Stop();
    }
    if (i == 30 && rank == 1 && workload_type == "tpcc-failover") {
      cout << i << ",Failing LM " << rank << endl;
      lock_manager->DisableRemoteAtomicAccess();
      lock_manager->Stop();
    }
    if (i == 45 && (rank == 2 || rank == 3) &&
        workload_type == "tpcc-failover") {
      cout << i << ",Failing LM " << rank << endl;
      lock_manager->DisableRemoteAtomicAccess();
      lock_manager->Stop();
    }
    if (i == 60 && (rank >= 4 && rank <= 7) &&
        workload_type == "tpcc-failover") {
      cout << i << ",Failing LM " << rank << endl;
      lock_manager->DisableRemoteAtomicAccess();
      lock_manager->Stop();
    }
    MPI_Barrier(MPI_COMM_WORLD);
    Poco::Thread::sleep(1000);
  }

  // Get CPU usage.
  usage.terminate = true;
  pthread_join(cpu_measure_thread, NULL);
  double server_cpu_usage = 0, client_cpu_usage = 0;
  int server_count = 0, client_count = 0;

  if (workload_type == "hotspot" || workload_type == "hotspot-exclusive" ||
      workload_type == "tpcc-hotspot" ||
      strncasecmp(workload_type.c_str(), "tpcc-hotspot", 12) == 0) {
    if (rank != 0) {
      for (int i = 0; i < num_users; ++i) {
        users[i]->Stop();
      }
      client_cpu_usage = usage.total_cpu / usage.num_sample;
    } else {
      server_cpu_usage = usage.total_cpu / usage.num_sample;
    }
    server_count = 1;
    client_count = num_managers - 1;
  } else if (workload_type == "tpcc-dist" ||
             strncasecmp(workload_type.c_str(), "tpcc-random", 11) == 0) {
    if (rank >= num_managers / 2) {
      for (int i = 0; i < num_users; ++i) {
        users[i]->Stop();
      }
      client_cpu_usage = usage.total_cpu / usage.num_sample;
    } else {
      server_cpu_usage = usage.total_cpu / usage.num_sample;
    }
    server_count = num_managers / 2;
    client_count = num_managers / 2;
  } else if (workload_type == "tpcc-dist2") {
    if (rank >= tpcc_dist2_num_server) {
      for (int i = 0; i < num_users; ++i) {
        users[i]->Stop();
      }
      client_cpu_usage = usage.total_cpu / usage.num_sample;
    } else {
      server_cpu_usage = usage.total_cpu / usage.num_sample;
    }
    server_count = num_managers / 2;
    client_count = num_managers / 2;
  } else if (workload_type == "tpcc-failover") {
    if (rank >= num_managers / 2) {
      for (int i = 0; i < num_users; ++i) {
        users[i]->Stop();
      }
      client_cpu_usage = usage.total_cpu / usage.num_sample;
    } else {
      server_cpu_usage = usage.total_cpu / usage.num_sample;
    }
    server_count = num_managers / 4;  // be careful
    client_count = num_managers / 2;
  } else {
    for (int i = 0; i < num_users; ++i) {
      users[i]->Stop();
    }
    client_cpu_usage = usage.total_cpu / usage.num_sample;
    server_cpu_usage = usage.total_cpu / usage.num_sample;
    server_count = num_managers;
    client_count = num_managers;
  }
  for (size_t i = 0; i < user_threads.size(); ++i) {
    try {
      user_threads[i]->join(5000);
    } catch (Poco::Exception& e) {
      std::cerr << e.displayText() << std::endl;
    }
  }
  // Poco::Thread::sleep(5000);

  MPI_Barrier(MPI_COMM_WORLD);

  // Simple sanity check.
  if (lock_mode != REMOTE_D2LM_V2) {
    if (lock_manager->lock_table_[0] != 0) {
      cerr << "Value of index 0 is not zero: " << lock_manager->lock_table_[0]
           << endl;
      exit(ERROR_FAILED_SANITY_CHECK);
    }
  } else {
    uint64_t value = lock_manager->lock_table_[0];
    uint64_t exclusive_number =
        (value & kExclusiveNumberBitMask) >> kExclusiveNumberBitShift;
    uint64_t shared_number =
        (value & kSharedNumberBitMask) >> kSharedNumberBitShift;
    uint64_t exclusive_max =
        (value & kExclusiveMaxBitMask) >> kExclusiveMaxBitShift;
    uint64_t shared_max = value & kSharedMaxBitMask;
    if (exclusive_number != exclusive_max || shared_number != shared_max) {
      cerr << "Counter numbers do not match: " << exclusive_number << ","
           << shared_number << "," << exclusive_max << "," << shared_max
           << endl;
      // exit(ERROR_FAILED_SANITY_CHECK);
    }
    for (int i = 0; i < num_managers; ++i) {
      if (i == rank) {
        cerr << "(Node " << i << ") Counter numbers: " << exclusive_number
             << "," << shared_number << "," << exclusive_max << ","
             << shared_max << endl;
      }
      MPI_Barrier(MPI_COMM_WORLD);
    }
  }

  double average_server_cpu_usage = 0;
  double average_client_cpu_usage = 0;
  double total_average_latency = 0;
  double total_average_99pct_latency = 0;
  double total_average_999pct_latency = 0;
  double average_latency = 0;
  double average_99pct_latency = 0;
  double average_999pct_latency = 0;
  double total_average_latency_with_contention = 0;
  double total_average_99pct_latency_with_contention = 0;
  double total_average_999pct_latency_with_contention = 0;
  double average_latency_with_contention = 0;
  double average_99pct_latency_with_contention = 0;
  double average_999pct_latency_with_contention = 0;
  double total_average_latency_with_backoff = 0;
  double total_average_99pct_latency_with_backoff = 0;
  double total_average_999pct_latency_with_backoff = 0;
  double average_latency_with_backoff = 0;
  double average_99pct_latency_with_backoff = 0;
  double average_999pct_latency_with_backoff = 0;
  double average_backoff_time = 0;
  double total_average_backoff_time = 0;

  double average_contention_count = 0;
  double total_average_contention_count = 0;
  double average_contention_count2 = 0;
  double total_average_contention_count2 = 0;
  double average_contention_count3 = 0;
  double total_average_contention_count3 = 0;
  double average_contention_count4 = 0;
  double total_average_contention_count4 = 0;
  double average_contention_count5 = 0;
  double total_average_contention_count5 = 0;
  double average_contention_count6 = 0;
  double total_average_contention_count6 = 0;

  uint64_t total_max_latency = 0;
  uint64_t max_latency = 0;
  uint64_t total_throughput = 0;
  uint64_t average_throughput = 0;
  uint64_t throughput_99pct = 0;
  uint64_t count = 0;
  uint64_t total_count = 0;
  uint64_t backoff_count = 0;
  uint64_t total_backoff_count = 0;
  uint64_t count_with_contention = 0;
  uint64_t total_count_with_contention = 0;
  uint64_t count_with_backoff = 0;
  uint64_t total_count_with_backoff = 0;

  uint64_t false_positives = 0;
  uint64_t total_false_positives = 0;

  // RDMA stats
  uint64_t rdma_read = lock_manager->GetTotalRDMAReadCount();
  uint64_t rdma_write = lock_manager->GetTotalRDMAWriteCount();
  uint64_t rdma_send = lock_manager->GetTotalRDMASendCount();
  uint64_t rdma_recv = lock_manager->GetTotalRDMARecvCount();
  uint64_t rdma_atomic_fa = lock_manager->GetTotalRDMAAtomicFACount();
  uint64_t rdma_atomic_cas = lock_manager->GetTotalRDMAAtomicCASCount();
  uint64_t total_rdma_read = 0;
  uint64_t total_rdma_write = 0;
  uint64_t total_rdma_send = 0;
  uint64_t total_rdma_recv = 0;
  uint64_t total_rdma_atomic_fa = 0;
  uint64_t total_rdma_atomic_cas = 0;
  uint64_t total_rdma_op = 0;

  for (int i = 0; i < num_users; ++i) {
    users[i]->SortLatency();
    count += users[i]->GetCount();
    backoff_count += users[i]->GetBackoffCount();
    count_with_contention += users[i]->GetCountWithContention();
    count_with_backoff += users[i]->GetCountWithBackoff();
    average_latency += users[i]->GetAverageLatency();
    average_99pct_latency += users[i]->Get99PercentileLatency();
    average_999pct_latency += users[i]->Get999PercentileLatency();
    average_latency_with_contention +=
        users[i]->GetAverageLatencyWithContention();
    average_99pct_latency_with_contention +=
        users[i]->Get99PercentileLatencyWithContention();
    average_999pct_latency_with_contention +=
        users[i]->Get999PercentileLatencyWithContention();
    average_latency_with_backoff += users[i]->GetAverageLatencyWithBackoff();
    average_99pct_latency_with_backoff +=
        users[i]->Get99PercentileLatencyWithBackoff();
    average_999pct_latency_with_backoff +=
        users[i]->Get999PercentileLatencyWithBackoff();
    average_backoff_time += users[i]->GetAverageBackoffTime();
    average_contention_count += users[i]->GetAverageContentionCount();
    average_contention_count2 += users[i]->GetAverageContentionCount2();
    average_contention_count3 += users[i]->GetAverageContentionCount3();
    average_contention_count4 += users[i]->GetAverageContentionCount4();
    average_contention_count5 += users[i]->GetAverageContentionCount5();
    average_contention_count6 += users[i]->GetAverageContentionCount6();
    max_latency = (max_latency < users[i]->GetMaxLatency())
                      ? users[i]->GetMaxLatency()
                      : max_latency;
    false_positives += users[i]->GetFalsePositives();
  }
  cout << "Count " << rank << " = " << count << endl;
  average_latency /= num_users;
  average_99pct_latency /= num_users;
  average_999pct_latency /= num_users;

  average_latency_with_contention /= num_users;
  average_99pct_latency_with_contention /= num_users;
  average_999pct_latency_with_contention /= num_users;

  average_latency_with_backoff /= num_users;
  average_99pct_latency_with_backoff /= num_users;
  average_999pct_latency_with_backoff /= num_users;

  MPI_Reduce(&server_cpu_usage, &average_server_cpu_usage, 1, MPI_DOUBLE,
             MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&client_cpu_usage, &average_client_cpu_usage, 1, MPI_DOUBLE,
             MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&average_latency, &total_average_latency, 1, MPI_DOUBLE, MPI_SUM,
             0, MPI_COMM_WORLD);
  MPI_Reduce(&average_backoff_time, &total_average_backoff_time, 1, MPI_DOUBLE,
             MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&average_99pct_latency, &total_average_99pct_latency, 1,
             MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&average_999pct_latency, &total_average_999pct_latency, 1,
             MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&max_latency, &total_max_latency, 1, MPI_LONG_LONG_INT, MPI_MAX, 0,
             MPI_COMM_WORLD);

  MPI_Reduce(&average_contention_count, &total_average_contention_count, 1,
             MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&average_contention_count2, &total_average_contention_count2, 1,
             MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&average_contention_count3, &total_average_contention_count3, 1,
             MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&average_contention_count4, &total_average_contention_count4, 1,
             MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&average_contention_count5, &total_average_contention_count5, 1,
             MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&average_contention_count6, &total_average_contention_count6, 1,
             MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

  MPI_Reduce(&average_latency_with_contention,
             &total_average_latency_with_contention, 1, MPI_DOUBLE, MPI_SUM, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(&average_99pct_latency_with_contention,
             &total_average_99pct_latency_with_contention, 1, MPI_DOUBLE,
             MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&average_999pct_latency_with_contention,
             &total_average_999pct_latency_with_contention, 1, MPI_DOUBLE,
             MPI_SUM, 0, MPI_COMM_WORLD);

  MPI_Reduce(&average_latency_with_backoff, &total_average_latency_with_backoff,
             1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&average_99pct_latency_with_backoff,
             &total_average_99pct_latency_with_backoff, 1, MPI_DOUBLE, MPI_SUM,
             0, MPI_COMM_WORLD);
  MPI_Reduce(&average_999pct_latency_with_backoff,
             &total_average_999pct_latency_with_backoff, 1, MPI_DOUBLE, MPI_SUM,
             0, MPI_COMM_WORLD);

  MPI_Reduce(&count, &total_count, 1, MPI_LONG_LONG_INT, MPI_SUM, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(&backoff_count, &total_backoff_count, 1, MPI_LONG_LONG_INT,
             MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&count_with_contention, &total_count_with_contention, 1,
             MPI_LONG_LONG_INT, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&count_with_backoff, &total_count_with_backoff, 1,
             MPI_LONG_LONG_INT, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&false_positives, &total_false_positives, 1, MPI_LONG_LONG_INT,
             MPI_SUM, 0, MPI_COMM_WORLD);

  MPI_Reduce(&rdma_read, &total_rdma_read, 1, MPI_LONG_LONG_INT, MPI_SUM, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(&rdma_write, &total_rdma_write, 1, MPI_LONG_LONG_INT, MPI_SUM, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(&rdma_send, &total_rdma_send, 1, MPI_LONG_LONG_INT, MPI_SUM, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(&rdma_recv, &total_rdma_recv, 1, MPI_LONG_LONG_INT, MPI_SUM, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(&rdma_atomic_cas, &total_rdma_atomic_cas, 1, MPI_LONG_LONG_INT,
             MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&rdma_atomic_fa, &total_rdma_atomic_fa, 1, MPI_LONG_LONG_INT,
             MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Barrier(MPI_COMM_WORLD);

  if (rank == 0) {
    average_server_cpu_usage /= server_count;
    average_client_cpu_usage /= client_count;
    total_average_latency /= num_managers;
    total_average_99pct_latency /= num_managers;
    total_average_999pct_latency /= num_managers;
    total_average_latency_with_contention /= num_managers;
    total_average_99pct_latency_with_contention /= num_managers;
    total_average_999pct_latency_with_contention /= num_managers;
    total_average_latency_with_backoff /= num_managers;
    total_average_99pct_latency_with_backoff /= num_managers;
    total_average_999pct_latency_with_backoff /= num_managers;
    total_average_backoff_time /= num_managers;

    total_rdma_op = total_rdma_read + total_rdma_write + total_rdma_send +
                    total_rdma_recv + total_rdma_atomic_fa +
                    total_rdma_atomic_cas;

    // Sort throughputs.
    std::sort(throughputs.begin(), throughputs.end());
    total_throughput =
        std::accumulate(throughputs.begin(), throughputs.end(), 0ULL);
    average_throughput = total_throughput / throughputs.size();
    throughput_99pct = throughputs[floor(throughputs.size() * 0.99)];
    cout << "Avg. Server CPU Usage = " << average_server_cpu_usage << " %"
         << endl;
    cout << "Avg. Client CPU Usage = " << average_client_cpu_usage << " %"
         << endl;
    cout << "Tx Count = " << total_count << endl;
    cout << "Tx Count With Contention = " << total_count_with_contention
         << endl;
    cout << "Tx Count With Backoff = " << total_count_with_backoff << endl;
    cout << "Backoff Count = " << total_backoff_count << endl;
    cout << "Avg. Throughput = " << average_throughput << endl;
    cout << "99pct Throughput = " << throughput_99pct << endl;
    cout << "Max Throughput = " << max_throughput << endl;
    cout << "Avg. Latency = " << total_average_latency << " us" << endl;
    cout << "Avg. 99pct Latency = " << total_average_99pct_latency << " us"
         << endl;
    cout << "Avg. 99.9pct Latency = " << total_average_999pct_latency << " us"
         << endl;
    cout << "Avg. Latency With Contention = "
         << total_average_latency_with_contention << " us" << endl;
    cout << "Avg. 99pct Latency With Contention = "
         << total_average_99pct_latency_with_contention << " us" << endl;
    cout << "Avg. 99.9pct Latency With Contention = "
         << total_average_999pct_latency_with_contention << " us" << endl;
    cout << "Avg. Latency With Backoff = " << total_average_latency_with_backoff
         << " us" << endl;
    cout << "Avg. 99pct Latency With Backoff = "
         << total_average_99pct_latency_with_backoff << " us" << endl;
    cout << "Avg. 99.9pct Latency With Backoff = "
         << total_average_999pct_latency_with_backoff << " us" << endl;
    cout << "Avg. Backoff Time = " << total_average_backoff_time << " us"
         << endl;
    cout << "Avg. Contention Count = "
         << total_average_contention_count + total_average_contention_count2 +
                total_average_contention_count3 +
                total_average_contention_count4 +
                total_average_contention_count5 +
                total_average_contention_count6
         << endl;
    cout << "Avg. Contention Count (type 1) = "
         << total_average_contention_count << endl;
    cout << "Avg. Contention Count (type 2) = "
         << total_average_contention_count2 << endl;
    cout << "Avg. Contention Count (type 3) = "
         << total_average_contention_count3 << endl;
    cout << "Avg. Contention Count (type 4) = "
         << total_average_contention_count4 << endl;
    cout << "Avg. Contention Count (type 5) = "
         << total_average_contention_count5 << endl;
    cout << "Avg. Contention Count (type 6) = "
         << total_average_contention_count6 << endl;
    cout << "Max Latency = " << total_max_latency << " us" << endl;
    cout << "Total RDMA Read = " << total_rdma_read << endl;
    cout << "Total RDMA Write = " << total_rdma_write << endl;
    cout << "Total RDMA Send = " << total_rdma_send << endl;
    cout << "Total RDMA Recv = " << total_rdma_recv << endl;
    cout << "Total RDMA CAS = " << total_rdma_atomic_cas << endl;
    cout << "Total RDMA FA = " << total_rdma_atomic_fa << endl;
    cout << "Total RDMA Op = " << total_rdma_op << endl;
    cout << "Total False Positives (D2LM Only) = " << total_false_positives
         << endl;

    // Print as CVS at the end.
    cout << "Workload, Think Time Type, Think Time Duration, Lock Mode, # "
            "Nodes, # Users Per Node, # Objects Per Node, "
         << "# Retry, "
         << "Uses Random Backoff, "
         << "Avg. Server CPU Usage, Avg. Client CPU Usage, Tx Count, Tx Count "
            "With Contention, "
         << "Tx Count With Backoff, "
         << "Backoff Count, "
         << "Avg. Throughput, 99pct Throughput, Max Throughput, "
         << "Avg. Latency, Avg. 99pct Latency, Avg. 99.9pct Latency, "
         << "Avg. Latency With Contention, "
         << "Avg. 99pct Latency With Contention, "
         << "Avg. 99.9pct Latency With Contention, "
         << "Avg. Latency With Backoff, "
         << "Avg. 99pct Latency With Backoff, "
         << "Avg. 99.9pct Latency With Backoff, "
         << "Avg. Backoff Time, "
         << "Avg. Contention Count, "
         << "Avg. Contention Count (type 1), "
         << "Avg. Contention Count (type 2), "
         << "Avg. Contention Count (type 3), "
         << "Avg. Contention Count (type 4), "
         << "Avg. Contention Count (type 5), "
         << "Avg. Contention Count (type 6), "
         << "Max Latency, "
         << "RDMA Read, "
         << "RDMA Write, "
         << "RDMA Send, "
         << "RDMA Recv, "
         << "RDMA CAS, "
         << "RDMA FA, "
         << "RDMA Total,"
         << "False Positives On Deadlock (D2LM Only)" << endl;
    cout << workload_type << "," << think_time_type << ","
         << think_time_duration << "," << lock_mode_str << "," << num_managers
         << "," << num_users << "," << num_lock_object << "," << num_retry
         << "," << random_backoff_str << "," << average_server_cpu_usage << ","
         << average_client_cpu_usage << "," << total_count << ","
         << total_count_with_contention << "," << total_count_with_backoff
         << "," << total_backoff_count << "," << average_throughput << ","
         << throughput_99pct << "," << max_throughput << ","
         << total_average_latency << "," << total_average_99pct_latency << ","
         << total_average_999pct_latency << ","
         << total_average_latency_with_contention << ","
         << total_average_99pct_latency_with_contention << ","
         << total_average_999pct_latency_with_contention << ","
         << total_average_latency_with_backoff << ","
         << total_average_99pct_latency_with_backoff << ","
         << total_average_999pct_latency_with_backoff << ","
         << total_average_backoff_time << ","
         << total_average_contention_count + total_average_contention_count2 +
                total_average_contention_count3 +
                total_average_contention_count4 +
                total_average_contention_count5 +
                total_average_contention_count6
         << "," << total_average_contention_count << ","
         << total_average_contention_count2 << ","
         << total_average_contention_count3 << ","
         << total_average_contention_count4 << ","
         << total_average_contention_count5 << ","
         << total_average_contention_count6 << "," << total_max_latency << ","
         << total_rdma_read << "," << total_rdma_write << "," << total_rdma_send
         << "," << total_rdma_recv << "," << total_rdma_atomic_cas << ","
         << total_rdma_atomic_fa << "," << total_rdma_op << ","
         << total_false_positives << "," << endl;
  }
  MPI_Finalize();
}

void* MeasureCPUUsage(void* args) {
  CPUUsage* usage = (CPUUsage*)args;

  usage->total_cpu = 0;
  usage->num_sample = 0;
  usage->terminate = false;

  int numProcessors;
  clock_t lastCPU, lastSysCPU, lastUserCPU, now;
  double percent;

  FILE* file;
  struct tms timeSample;
  char line[128];

  lastCPU = times(&timeSample);
  lastSysCPU = timeSample.tms_stime;
  lastUserCPU = timeSample.tms_utime;

  file = fopen("/proc/cpuinfo", "r");
  numProcessors = 0;
  percent = 0;
  while (fgets(line, 128, file) != NULL) {
    if (strncmp(line, "processor", 9) == 0) numProcessors++;
  }
  fclose(file);

  while (!usage->terminate) {
    now = times(&timeSample);
    if (now <= lastCPU || timeSample.tms_stime < lastSysCPU ||
        timeSample.tms_utime < lastUserCPU) {
      // Overflow detection. Just skip this value.
      percent = -1.0;
    } else {
      percent = (timeSample.tms_stime - lastSysCPU) +
                (timeSample.tms_utime - lastUserCPU);
      percent /= (now - lastCPU);
      // percent /= numProcessors;
      percent *= 100;

      usage->total_cpu += percent;
      usage->num_sample += 1;
    }
    lastCPU = now;
    lastSysCPU = timeSample.tms_stime;
    lastUserCPU = timeSample.tms_utime;

    sleep(1);
  }
  return NULL;
}
