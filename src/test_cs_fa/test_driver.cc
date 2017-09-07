#include <pthread.h>
#include <vector>
#include "constants.h"
#include "limits.h"
#include "mpi.h"
#include "test_client.h"
#include "test_server.h"

using namespace std;
using namespace rdma::test;

void* RunServer(void*);
void* RunClient(void*);

int main(int argc, char** argv) {
  MPI_Init(&argc, &argv);
  if (argc != 5) {
    cout << "USAGE: " << argv[0]
         << " <work_dir> <test_mode> <num_client_thread_per_node> <max_count>"
         << endl;
    exit(1);
  }
  int mode = atoi(argv[2]);
  int num_thread = atoi(argv[3]);
  uint64_t max_count = atol(argv[4]);

  int rank, num_nodes;
  MPI_Comm_size(MPI_COMM_WORLD, &num_nodes);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  TestServer* server = NULL;
  vector<TestClient*> clients;

  if (rank == 0) {
    server = new TestServer(argv[1], mode, 1);
    switch (mode) {
      case TEST_RC_READ:
        cout << "Testing RC READ" << endl;
        break;
      case TEST_UC_WRITE:
        cout << "Testing UC WRITE" << endl;
        break;
      case TEST_RC_WRITE:
        cout << "Testing RC WRITE" << endl;
        break;
      case TEST_RC_SEND_RECV:
        cout << "Testing RC SEND/RECV" << endl;
        break;
      case TEST_RC_ATOMIC_FA:
        cout << "Testing RC ATOMIC (FA)" << endl;
        break;
      case TEST_RC_ATOMIC_CAS:
        cout << "Testing RC ATOMIC (CAS)" << endl;
        break;
      default:
        cout << "Unsupported test mode" << endl;
        exit(-1);
    }

    pthread_t server_thread;

    if (pthread_create(&server_thread, NULL, RunServer, (void*)server)) {
      cerr << "pthread_create() error." << endl;
      exit(-1);
    }
  }
  MPI_Barrier(MPI_COMM_WORLD);
  if (rank != 0) {
    sleep(1);
    for (int i = 0; i < num_thread; ++i) {
      TestClient* client = new TestClient(argv[1], mode, atol(argv[4]));
      clients.push_back(client);
    }
    for (int i = 0; i < num_thread; ++i) {
      pthread_t client_thread;
      if (pthread_create(&client_thread, NULL, RunClient, (void*)clients[i])) {
        cerr << "pthread_create() error." << endl;
        exit(-1);
      }
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);

  uint64_t* op_count = new uint64_t[65536];
  memset(op_count, 0x00, sizeof(uint64_t) * 65536);
  int time_taken = 0;
  int min_time_taken = 0;
  bool is_done = true;

  double local_total_time_taken = 0;
  double global_total_time_taken = 0;
  if (rank != 0) {
    while (true) {
      is_done = true;
      for (size_t i = 0; i < clients.size(); ++i) {
        op_count[time_taken] += clients[i]->GetCount();
        if (!clients[i]->isDone()) {
          is_done = false;
        }
      }
      cout << time_taken << ": " << op_count[time_taken] << endl;
      ++time_taken;
      if (is_done) break;
      sleep(1);
    }
  }

  if (time_taken == 0 || rank == 0) time_taken = INT_MAX;
  MPI_Barrier(MPI_COMM_WORLD);

  MPI_Allreduce(&time_taken, &min_time_taken, 1, MPI_INT, MPI_MIN,
                MPI_COMM_WORLD);

  uint64_t* local_throughput = new uint64_t[min_time_taken];
  uint64_t* global_throughput = new uint64_t[min_time_taken];

  for (int i = 0; i < min_time_taken; ++i) {
    if (i == 0) {
      local_throughput[i] = op_count[i];
    } else {
      local_throughput[i] = op_count[i] - op_count[i - 1];
    }
  }

  if (rank == 0) {
    cout << "min time taken = " << min_time_taken << endl;
  }

  MPI_Barrier(MPI_COMM_WORLD);
  for (int i = 0; i < min_time_taken; ++i) {
    uint64_t local = local_throughput[i];
    uint64_t global;
    MPI_Reduce(&local, &global, 1, MPI_LONG_LONG_INT, MPI_SUM, 0,
               MPI_COMM_WORLD);
    global_throughput[i] = global;
    MPI_Barrier(MPI_COMM_WORLD);
  }

  uint64_t max_throughput = 0;
  if (rank == 0) {
    for (int i = 0; i < min_time_taken; ++i) {
      // cout << "throughput = " << global_throughput[i] << endl;
      if (max_throughput < global_throughput[i]) {
        max_throughput = global_throughput[i];
      }
    }
  } else {
    for (int i = 0; i < num_thread; ++i) {
      local_total_time_taken += clients[i]->GetTotalTimeTaken();
    }
  }
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Reduce(&local_total_time_taken, &global_total_time_taken, 1, MPI_DOUBLE,
             MPI_SUM, 0, MPI_COMM_WORLD);

  if (rank == 0) {
    cout << "Max Throughput = " << max_throughput << " ops" << endl;
    cout << "Average Time Taken = "
         << (global_total_time_taken / (double)((num_node - 1) * num_thread)) /
                (1000 * 1000 * 1000)
         << " s" << endl;
    if (server) server->SetDone(true);
    sleep(1);
  }

  double* all_latency = new double[max_count * num_thread];
  for (size_t i = 0; i < clients.size(); ++i) {
    double* latency = clients[i]->GetLatency();
    for (size_t j = 0; j < max_count; ++j) {
      all_latency[i * max_count + j] = latency[j];
    }
  }
  MPI_Barrier(MPI_COMM_WORLD);

  double latency_total = 0;
  if (rank == 0) {
    double* recv_latency = new double[max_count * num_thread];
    for (int i = 1; i < num_nodes; ++i) {
      MPI_Status status;
      MPI_Recv(recv_latency, max_count * num_thread, MPI_DOUBLE, i, 1,
               MPI_COMM_WORLD, &status);
      for (size_t j = 0; j < max_count * num_thread; ++j) {
        cerr << recv_latency[j] << endl;
        latency_total += recv_latency[j];
      }
    }
    cout << "Average Latency = "
         << latency_total / (num_thread * max_count) / 1000 << " us" << endl;
    delete[] recv_latency;
  } else {
    MPI_Send(all_latency, max_count * num_thread, MPI_DOUBLE, 0, 1,
             MPI_COMM_WORLD);
  }

  delete[] all_latency;

  MPI_Finalize();
  return 0;
}

void* RunServer(void* args) {
  TestServer* server = (TestServer*)args;
  server->Run();
  return NULL;
}

void* RunClient(void* args) {
  TestClient* client = (TestClient*)args;
  client->Run();
  return NULL;
}
