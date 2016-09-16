#include <pthread.h>
#include "constants.h"
#include "test_server.h"
#include "test_client.h"

using namespace std;
using namespace rdma::test;

void* RunServer(void*);
void* RunClient(void*);

int main(int argc, char** argv) {
  if (argc != 4) {
    cout << "USAGE: " << argv[0] << " <work_dir> <test_mode> <max_count>" << endl;
    exit(1);
  }
  int mode = atoi(argv[2]);
  TestServer* server = new TestServer(argv[1], mode, 1024);
  TestClient* client = new TestClient(argv[1], mode, atol(argv[3]));

  switch(mode) {
    case TEST_RC_READ:
      cout << "Testing RC READ" << endl;
      break;
    case TEST_UC_WRITE:
      cout << "Testing UC WRITE" << endl;
      break;
    case TEST_RC_WRITE:
      cout << "Testing RC WRITE" << endl;
      break;
    default:
      cout << "Unsupported test mode" << endl;
      exit(-1);
  }

  pthread_t server_thread;
  pthread_t client_thread;

  if (pthread_create(&server_thread, NULL, RunServer, (void*)server)) {
    cerr << "pthread_create() error." << endl;
    exit(-1);
  }

  sleep(1);

  if (pthread_create(&client_thread, NULL, RunClient,
        (void*)client)) {
    cerr << "pthread_create() error." << endl;
    exit(-1);
  }

  sleep(1);
  while (true) {
    sleep(1);
  }

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
