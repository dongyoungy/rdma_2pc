#include <pthread.h>
#include <algorithm>
#include "constants.h"
#include "test_server.h"
#include "test_client.h"

using namespace std;
using namespace rdma::test;

void* RunServer(void*);
void* RunClient(void*);

int main(int argc, char** argv) {
  if (argc != 4) {
    cout << "USAGE: " << argv[0] << " <work_dir> <data_size> <percentage>" << endl;
    exit(1);
  }
  size_t data_size = atol(argv[2]);
  int percentage = atol(argv[3]);

  if (percentage < 1 || percentage > 100) {
    cerr << "percentage must be between 1 and 100" << endl;
    exit(-1);
  }

  double min_percentage = 50.0 - (percentage/2.0);
  double max_percentage = 50.0 + (percentage/2.0);
  int min_value = data_size * (min_percentage/100.0);
  int max_value = data_size * (max_percentage/100.0);

  //cout << "data size = " << data_size << endl;
  //cout << "min range = " << min_value << endl;
  //cout << "max range = " << max_value << endl;

  srand(10000);
  int* data = new int[data_size];
  int* sorted_data = new int[data_size];

  for (int i=0;i<data_size;++i) {
    data[i] = 1 + (rand() % data_size);
  }

  memcpy(sorted_data, data, sizeof(int)*data_size);
  sort(sorted_data, sorted_data+data_size);

  //cout << "<original>" << endl;
  //for (int i=0;i<data_size;++i) {
    //cout << sorted_data[i] << " ";
  //}
  //cout << endl;

  TestServer* server = new TestServer(argv[1], sorted_data, data_size);
  TestClient* client = new TestClient(argv[1], data, data_size);

  int* res = new int[data_size];
  int* range_data_local = new int[data_size];
  int* range_data_remote = new int[data_size];

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


  struct timespec start, end;

  clock_gettime(CLOCK_MONOTONIC, &start);
  res = client->GetDataFromRangeLocal(min_value, max_value);
  clock_gettime(CLOCK_MONOTONIC, &end);

  double dt = ((double)end.tv_sec *1.0e+9 + end.tv_nsec) -
    ((double)start.tv_sec * 1.0e+9 + start.tv_nsec);
  double local = dt;

  //memcpy(range_data_local, res, sizeof(int)*data_size);

  clock_gettime(CLOCK_MONOTONIC, &start);
  res = client->GetDataFromRangeRemote(min_value, max_value);
  clock_gettime(CLOCK_MONOTONIC, &end);

  dt = ((double)end.tv_sec *1.0e+9 + end.tv_nsec) -
    ((double)start.tv_sec * 1.0e+9 + start.tv_nsec);
  double remote = dt;

  //memcpy(range_data_remote, res, sizeof(int)*data_size);

  cout << data_size << "," << local << "," << remote << endl;

  //cout << "<local>" << endl;
  //for (int i = 0; i < data_size; ++i) {
    //cout << range_data_local[i] << " ";
  //}
  //cout << endl;
  //cout << "<remote>" << endl;
  //for (int i = 0; i < data_size; ++i) {
    //cout << range_data_remote[i] << " ";
  //}
  //cout << endl;

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
