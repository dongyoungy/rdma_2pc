#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "mpi.h"
#include "test_client.h"

using namespace std;
using namespace rdma::test;

int main(int argc, char** argv) {

  MPI_Init(&argc, &argv);

  if (argc != 5) {
    cout << argv[0] << " <work_dir> <test_mode> <test_duration (s)> <data_size>"
      << endl;
    cout << "test_mode: 0 = semaphore, 1 = data" << endl;
    exit(1);
  }

  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  printf("test_client running with rank %d out of %d nodes", rank, world_size);

  TestClient client(argv[1], atoi(argv[2]), atoi(argv[3]), atoll(argv[4]));
  client.Run();

  MPI_Finalize();

  return 0;
}
