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

    Poco::Thread server_thread;
    server_thread.start(*server);
  }
  MPI_Barrier(MPI_COMM_WORLD);
  if (rank != 0) {
    sleep(2);
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

  Poco::Thread::sleep(duration * 1000);
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

  if (rank == 0) {
    total_average_latency /= (double)(num_nodes - 1);
    cout << "# of Clients = " << (num_nodes - 1) * num_thread << endl;
    cout << "Duration = " << duration << " s" << endl;
    cout << "Total Throughput = " << total_count << endl;
    cout << "Overall Average Latency = " << total_average_latency << endl;
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
