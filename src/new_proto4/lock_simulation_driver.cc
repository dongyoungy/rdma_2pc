#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <limits.h>
#include <pthread.h>
#include <sys/times.h>
#include <cmath>
#include <iostream>
#include <numeric>
#include <vector>

#include "Poco/Thread.h"

#include "hotspot_lock_simulator.h"
#include "lock_manager.h"
#include "lock_simulator.h"
#include "mpi.h"
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
  MPI_Init(&argc, &argv);

  if (argc != 11) {
    cout << "USAGE: " << argv[0] << " <work_dir> <num_lock_object> <duration>"
         << " <request_size> <num_users> <lock_mode>"
         << " <num_retry>"
         << " <workload_type> <think_time_type> <enable_random_backoff> "
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
  string random_backoff_str = argv[k++];

  if (num_managers > 32) {
    cerr << "# of nodes must be less than 33" << endl;
    exit(-1);
  }

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

  if (lock_mode_str == "traditional") {
    lock_mode = PROXY_QUEUE;
  } else if (lock_mode_str == "simple_retry") {
    lock_mode = REMOTE_POLL;
    LockManager::SetSharedExclusiveRule("fail");
    LockManager::SetExclusiveSharedRule("fail");
    LockManager::SetExclusiveExclusiveRule("fail");
  } else if (lock_mode_str == "drtm") {
    lock_mode = REMOTE_POLL;
    LockManager::SetSharedExclusiveRule("fail");
    LockManager::SetExclusiveSharedRule("poll");
    LockManager::SetExclusiveExclusiveRule("fail");
  } else if (lock_mode_str == "ncosed") {
    lock_mode = REMOTE_NOTIFY;
  } else if (lock_mode_str == "d2lm") {
    lock_mode = REMOTE_QUEUE;
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

  if (rank == 0) {
    cout << "Type of Workload = " << workload_type << endl;
    cout << "Type of Think Time = " << think_time_type << endl;
    cout << "Duration = " << duration << " s" << endl;
    cout << "# Requests per Tx = " << request_size << endl;
    cout << "# Lock Objects = " << num_lock_object << endl;
    cout << "Lock Mode = " << lock_mode_str << endl;
  }

  LockManager::SetFailRetry(num_retry);
  LockManager::SetPollRetry(num_retry);

  std::unique_ptr<LockManager> lock_manager(
      new LockManager(argv[1], rank, num_managers, num_lock_object, lock_mode));

  LockManager* temp_manager = lock_manager.get();
  if (lock_manager->Initialize()) {
    cerr << "LockManager initialization failure." << endl;
    exit(-1);
  }

  Poco::Thread manager_thread;
  manager_thread.start(*lock_manager);

  std::vector<std::unique_ptr<LockSimulator>> users;
  LockSimulator* temp_user;
  for (int i = 0; i < num_users; ++i) {
    std::unique_ptr<LockSimulator> simulator;
    // TODO: Add other simulators.
    if (workload_type == "simple") {
      simulator.reset(new LockSimulator(lock_manager.get(), num_managers,
                                        num_lock_object, request_size,
                                        think_time_type, do_random_backoff));
    } else if (workload_type == "hotspot") {
      simulator.reset(new HotspotLockSimulator(
          lock_manager.get(), num_managers, num_lock_object, request_size,
          think_time_type, do_random_backoff));
    } else if (workload_type == "tpcc-uniform") {
      simulator.reset(new TPCCLockSimulator(lock_manager.get(), num_managers,
                                            kTPCCNumObjects, think_time_type,
                                            do_random_backoff, rank));
    } else if (workload_type == "tpcc-hotspot") {
      simulator.reset(new TPCCLockSimulator(lock_manager.get(), 1,
                                            kTPCCNumObjects, think_time_type,
                                            do_random_backoff, 0));
    } else {
      cerr << "Unknown workload: " << workload_type << endl;
      exit(-2);
    }
    lock_manager->RegisterUser(i + 1, simulator.get());
    temp_user = simulator.get();
    users.push_back(std::move(simulator));
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
  std::vector<std::unique_ptr<Poco::Thread>> user_threads;
  for (int i = 0; i < num_users; ++i) {
    std::unique_ptr<Poco::Thread> user_thread(new Poco::Thread);
    user_thread->start(*users[i]);
    user_threads.push_back(std::move(user_thread));
  }

  int zero_count = 0;
  for (int i = 0; i < duration; ++i) {
    current_count = 0;
    for (int j = 0; j < num_users; ++j) {
      current_count += users[j]->GetCount();
    }
    MPI_Gather(&current_count, 1, MPI_LONG_LONG_INT, current_counts, 1,
               MPI_LONG_LONG_INT, 0, MPI_COMM_WORLD);

    if (rank == 0) {
      current_total_count = 0;
      for (int j = 0; j < num_managers; ++j) {
        current_total_count += current_counts[j];
      }
      uint64_t current_throughput = current_total_count - last_total_count;
#ifdef VERBOSE
      cout << "throughput = " << current_throughput << endl;
#endif
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
        // abort();
      }
#endif
    }
    MPI_Barrier(MPI_COMM_WORLD);
    Poco::Thread::sleep(1000);
  }

  for (int i = 0; i < num_users; ++i) {
    users[i]->Stop();
  }
  for (int i = 0; i < num_users; ++i) {
    try {
      user_threads[i]->join(5000);
    } catch (Poco::Exception& e) {
      std::cerr << e.displayText() << std::endl;
    }
  }

  // Get CPU usage.
  usage.terminate = true;
  pthread_join(cpu_measure_thread, NULL);
  double cpu_usage = usage.total_cpu / usage.num_sample;

  MPI_Barrier(MPI_COMM_WORLD);

  // Simple sanity check.
  if (lock_manager->lock_table_[0] != 0) {
    cerr << "Value of index 0 is not zero: " << lock_manager->lock_table_[0]
         << endl;
    exit(ERROR_FAILED_SANITY_CHECK);
  }

  double average_cpu_usage = 0;
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
  uint64_t total_max_latency = 0;
  uint64_t max_latency = 0;
  uint64_t total_throughput = 0;
  uint64_t average_throughput = 0;
  uint64_t throughput_99pct = 0;
  uint64_t count = 0;
  uint64_t total_count = 0;
  uint64_t count_with_contention = 0;
  uint64_t total_count_with_contention = 0;
  uint64_t count_with_backoff = 0;
  uint64_t total_count_with_backoff = 0;
  for (int i = 0; i < num_users; ++i) {
    users[i]->SortLatency();
    count += users[i]->GetCount();
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
    max_latency = (max_latency < users[i]->GetMaxLatency())
                      ? users[i]->GetMaxLatency()
                      : max_latency;
  }
  average_latency /= num_users;
  average_99pct_latency /= num_users;
  average_999pct_latency /= num_users;

  average_latency_with_contention /= num_users;
  average_99pct_latency_with_contention /= num_users;
  average_999pct_latency_with_contention /= num_users;

  average_latency_with_backoff /= num_users;
  average_99pct_latency_with_backoff /= num_users;
  average_999pct_latency_with_backoff /= num_users;

  MPI_Reduce(&cpu_usage, &average_cpu_usage, 1, MPI_DOUBLE, MPI_SUM, 0,
             MPI_COMM_WORLD);
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
  MPI_Reduce(&count_with_contention, &total_count_with_contention, 1,
             MPI_LONG_LONG_INT, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&count_with_backoff, &total_count_with_backoff, 1,
             MPI_LONG_LONG_INT, MPI_SUM, 0, MPI_COMM_WORLD);

  if (rank == 0) {
    average_cpu_usage /= num_managers;
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
    // Sort throughputs.
    std::sort(throughputs.begin(), throughputs.end());
    total_throughput =
        std::accumulate(throughputs.begin(), throughputs.end(), 0ULL);
    average_throughput = total_throughput / throughputs.size();
    throughput_99pct = throughputs[floor(throughputs.size() * 0.99)];
    cout << "Avg. CPU Usage = " << average_cpu_usage << " %" << endl;
    cout << "Tx Count = " << total_count << endl;
    cout << "Tx Count With Contention = " << total_count_with_contention
         << endl;
    cout << "Tx Count With Backoff = " << total_count_with_backoff << endl;
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
    cout << "Max Latency = " << total_max_latency << " us" << endl;

    // Print as CVS at the end.
    cout << "Workload, Think Time, Lock Mode, # Nodes, # Objects Per Node, "
         << "# Retry, "
         << "Uses Backoff, "
         << "Avg. CPU Usage, Tx Count, Tx Count With Contention, "
         << "Tx Count With Backoff, "
         << "Avg. Throughput, 99pct Throughput, Max Throughput, "
         << "Avg. Latency, Avg. 99pct Latency, Avg. 99.9pct Latency, "
         << "Avg. Latency With Contention, "
         << "Avg. 99pct Latency With Contention, "
         << "Avg. 99.9pct Latency With Contention, "
         << "Avg. Latency With Backoff, "
         << "Avg. 99pct Latency With Backoff, "
         << "Avg. 99.9pct Latency With Backoff, "
         << "Avg. Backoff Time, "
         << "Max Latency" << endl;
    cout << workload_type << "," << think_time_type << "," << lock_mode_str
         << "," << num_managers << "," << num_lock_object << "," << num_retry
         << "," << random_backoff_str << "," << average_cpu_usage << ","
         << total_count << "," << total_count_with_contention << ","
         << total_count_with_backoff << "," << average_throughput << ","
         << throughput_99pct << "," << max_throughput << ","
         << total_average_latency << "," << total_average_99pct_latency << ","
         << total_average_999pct_latency << ","
         << total_average_latency_with_contention << ","
         << total_average_99pct_latency_with_contention << ","
         << total_average_999pct_latency_with_contention << ","
         << total_average_latency_with_backoff << ","
         << total_average_99pct_latency_with_backoff << ","
         << total_average_999pct_latency_with_backoff << ","
         << total_average_backoff_time << "," << max_latency << endl;
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
