#include <stdlib.h>
#include "test_server.h"

using namespace rdma::test;

int main(int argc, char** argv) {
  if (argc != 3) {
    cout << "USAGE: " << argv[0] << "<work_dir> <data_size>" << endl;
    exit(1);
  }
  TestServer server(argv[1], (size_t)atoll(argv[2]));
  server.Run();
}
