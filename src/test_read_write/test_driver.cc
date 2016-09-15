#include <pthread.h>
#include "test_server.h"
#include "test_client.h"

using namespace std;
using namespace rdma::test;

void* RunServer(void*);
void* RunClient(void*);

int main(int argc, char** argv) {
  if (argc != 2) {
    cout << "USAGE: " << argv[0] << " <work_dir>" << endl;
    exit(1);
  }
  TestServer* server = new TestServer(argv[1], 1024);
  TestClient* add_client = new TestClient(argv[1],
      TestClient::TEST_MODE_ADD_SEM);
  TestClient* reset_client = new TestClient(argv[1],
      TestClient::TEST_MODE_RESET_SEM);

  pthread_t server_thread;
  pthread_t add_client_thread;
  pthread_t reset_client_thread;

  if (pthread_create(&server_thread, NULL, RunServer, (void*)server)) {
    cerr << "pthread_create() error." << endl;
    exit(-1);
  }

  sleep(1);

  if (pthread_create(&add_client_thread, NULL, RunClient,
        (void*)add_client)) {
    cerr << "pthread_create() error." << endl;
    exit(-1);
  }

  sleep(1);
  while (server->GetSemaphore() == 0) {
    sleep(1);
  }

  if (pthread_create(&reset_client_thread, NULL, RunClient,
        (void*)reset_client)) {
    cerr << "pthread_create() error." << endl;
    exit(-1);
  }

  while (true) {
    if (reset_client->IsSemReset()) {
      add_client->StopAddingSem();
      break;
    }
    usleep(100000);
  }

  cout << "Current Semaphore = " << server->GetSemaphore() << endl;
  cout << "Client Added Semaphore = " << add_client->GetNumAddedSemaphore() << endl;

  return 0;
}

void* RunServer(void* args) {
  TestServer* server = (TestServer*)args;
  server->Run();
}

void* RunClient(void* args) {
  TestClient* client = (TestClient*)args;
  client->Run();
}
