#include <stdlib.h>
#include "test_server.h"

using namespace rdma::test;

int main(int argc, char** argv) {
  if (argc != 2) {
    cout << "USAGE: " << argv[0] << " <data_size>" << endl;
    exit(1);
  }
  TestServer server((size_t)atoll(argv[1]));
  server.Run();
}
