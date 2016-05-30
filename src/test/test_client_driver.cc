#include <iostream>
#include "test_client.h"

using namespace std;
using namespace rdma::test;

int main(int argc, char** argv) {
  if (argc != 3) {
    cout << argv[0] << " <server_name> <server_port>" << endl;
    exit(1);
  }

  TestClient client(argv[1], argv[2]);
  client.Run();

  return 0;
}
