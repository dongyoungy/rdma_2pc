#include <iostream>
#include <stdlib.h>
#include "test_client.h"

using namespace std;
using namespace rdma::test;

int main(int argc, char** argv) {
  if (argc != 5) {
    cout << argv[0] << "<server_name> <server_port> <test_mode> <data_size>"
      << endl;
    cout << "test_mode: 0 = semaphore, 1 = data"
    exit(1);
  }

  TestClient client(argv[1], argv[2], atoi(argv[3]), atoll(argv[4]));
  client.Run();

  return 0;
}
