#include <pthread.h>
#include <memory>
#include <vector>
#include "constants.h"
#include "limits.h"
#include "mpi.h"
#include "test_client.h"
#include "test_server.h"

#include "Poco/Thread.h"

using namespace std;
using namespace rdma::test;

void* RunServer(void*);
void* RunClient(void*);

int main(int argc, char** argv) {
  MPI_Init(&argc, &argv);
  if (argc != 5) {
    cout << "USAGE: " << argv[0]
         << " <work_dir> <test_mode> <num_client_thread_per_node> <duration>"
         << endl;
    exit(1);
  }
  string work_dir(argv[1]);
  string mode(argv[2]);
  int num_thread = atoi(argv[3]);
  int duration = atoi(argv[4]);

  int rank, num_nodes;
  MPI_Comm_size(MPI_COMM_WORLD, &num_nodes);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  std::unique_ptr<TestServer> server;
  std::vector<std::unique_ptr<TestClient>> clients;
  std::vector<std::unique_ptr<Poco::Thread>> client_threads;

  if (rank == 0) {
    server.reset(std::move(new TestServer(work_dir, mode)));
    cout << "# nodes = " << num_nodes << endl;
    cout << "Testing mode: " << mode << endl;
    cout << "Server PID: " << getpid() << endl;

    try {
      Poco::Thread server_thread;
      server_thread.start(*server);
    } catch (Poco::Exception& e) {
      cerr << e.displayText() << endl;
    }
  }
  if (rank != 0) {
    sleep(10);
    for (int i = 0; i < num_thread; ++i) {
      std::unique_ptr<TestClient> client(new TestClient(work_dir, mode));
      std::unique_ptr<Poco::Thread> client_thread(new Poco::Thread);
      client_thread->start(*client);
      clients.push_back(std::move(client));
      client_threads.push_back(std::move(client_thread));
    }
    cout << "client " << rank << " successfully started." << endl;
    cout << "PID: " << getpid() << endl;
  }

  MPI_Barrier(MPI_COMM_WORLD);

  uint64_t max_throughput = 0;
  uint64_t current_count = 0;
  uint64_t current_total_count = 0;
  uint64_t last_total_count = 0;
  uint64_t* current_counts = NULL;

  if (rank == 0) {
    current_counts = new uint64_t[num_nodes];
  }

  // Get max throughput.
  for (int i = 0; i < duration; ++i) {
    if (rank != 0) {
      current_count = 0;
      for (int i = 0; i < num_thread; ++i) {
        current_count += clients[i]->GetCount();
      }
    }
    MPI_Gather(&current_count, 1, MPI_LONG_LONG_INT, current_counts, 1,
               MPI_LONG_LONG_INT, 0, MPI_COMM_WORLD);
    if (rank == 0) {
      current_total_count = 0;
      for (int i = 0; i < num_nodes; ++i) {
        current_total_count += current_counts[i];
      }
      uint64_t current_throughput = current_total_count - last_total_count;
      if (current_throughput > max_throughput) {
        max_throughput = current_throughput;
      }
      last_total_count = current_total_count;
    }
    Poco::Thread::sleep(1000);
  }

  if (rank != 0) {
    for (int i = 0; i < num_thread; ++i) {
      clients[i]->Stop();
    }
    for (int i = 0; i < num_thread; ++i) {
      client_threads[i]->tryJoin(5 * 1000);
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);

  uint64_t count = 0;
  uint64_t total_count = 0;
  double average_latency = 0;
  double total_average_latency = 0;
  if (rank != 0) {
    for (int i = 0; i < num_thread; ++i) {
      count += clients[i]->GetCount();
      average_latency +=
          (clients[i]->GetTotalTimeTaken() / (double)clients[i]->GetCount());
    }
    average_latency /= num_thread;
    cout << "count = " << count << endl;
    cout << "average latency = " << average_latency << endl;
  }

  MPI_Reduce(&count, &total_count, 1, MPI_LONG_LONG_INT, MPI_SUM, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(&average_latency, &total_average_latency, 1, MPI_DOUBLE, MPI_SUM,
             0, MPI_COMM_WORLD);

  MPI_Barrier(MPI_COMM_WORLD);
  if (rank == 0) {
    sleep(3);
    total_average_latency /= (double)(num_nodes - 1);
    cout << "# of Clients = " << (num_nodes - 1) * num_thread << endl;
    cout << "Duration = " << duration << " s" << endl;
    cout << "Total Op Count = " << total_count << endl;
    cout << "Max Throughput (op/s) = " << max_throughput << endl;
    cout << "Overall Average Latency (us) = " << total_average_latency << endl;
    if (server) server->SetDone(true);
  }

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
