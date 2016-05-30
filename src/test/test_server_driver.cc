#include "test_server.h"

using namespace rdma::test;

int main(int argc, char** argv) {
  TestServer server;
  server.Run();
}
