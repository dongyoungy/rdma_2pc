#ifndef RDMA_PROTO_D2LMLOCKMANAGER_H
#define RDMA_PROTO_D2LMLOCKMANAGER_H

#include "lock_manager.h"

using namespace std;

namespace rdma {
namespace proto {

class LockSimulator;
class LockClient;
class CommunicationClient;

class D2LMLockManager : public LockManager {
 public:
  D2LMLockManager(const string& work_dir, uint32_t rank, int num_manager,
                  int num_lock_object, LockMode lock_mode,
                  int num_total_user = 0, int num_client = 1);

 protected:
  virtual int RegisterMemoryRegion(Context* context);
};

}  // namespace proto
}  // namespace rdma

#endif
