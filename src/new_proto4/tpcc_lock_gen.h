#ifndef RDMA_PROTO_TPCCLOCKGEN_H
#define RDMA_PROTO_TPCCLOCKGEN_H

#include <stdlib.h>
#include <cstddef>
#include <iostream>
#include <memory>
#include <vector>

#include "constants.h"
#include "lock_request.h"

using namespace std;

namespace rdma {
namespace proto {

class TPCCLockGen {
 public:
  TPCCLockGen(int home_warehouse_id, int num_warehouse);
  ~TPCCLockGen();
  int Generate(std::vector<std::unique_ptr<LockRequest>>& requests);

  static const int NEW_ORDER = 1;
  static const int PAYMENT = 2;
  static const int ORDER_STATUS = 3;
  static const int DELIVERY = 4;
  static const int STOCK_LEVEL = 5;

  static const int WAREHOUSE_START_IDX = 0;
  static const int DISTRICT_START_IDX = 1;
  static const int CUSTOMER_START_IDX = 11;
  static const int ORDER_START_IDX = 30011;
  static const int HISTORY_START_IDX = 60011;
  static const int NEW_ORDER_START_IDX = 90011;
  static const int ORDER_LINE_START_IDX = 120011;
  static const int STOCK_START_IDX = 420011;
  static const int ITEM_START_IDX = 520011;

  static const int NUM_ROW_WAREHOUSE = 1;
  static const int NUM_ROW_DISTRICT = 10;
  static const int NUM_ROW_CUSTOMER = 30000;
  static const int NUM_ROW_ORDER = 30000;
  static const int NUM_ROW_HISTORY = 30000;
  static const int NUM_ROW_NEW_ORDER = 30000;
  static const int NUM_ROW_ORDER_LINE = 300000;
  static const int NUM_ROW_STOCK = 100000;
  static const int NUM_ROW_ITEM = 100000;

  static const int NUM_ORDER_LINE_PER_ORDER = 10;
  static const int NUM_ORDER_LINE_PER_STOCK = 3;
  static const int NUM_HISTORY_PER_CUSTOMER = 1;
  static const int NUM_CUSTOMER_PER_DISTRICT = 3000;

 private:
  int GenerateNewOrder(std::vector<std::unique_ptr<LockRequest>>& requests);
  int GeneratePayment(std::vector<std::unique_ptr<LockRequest>>& requests);
  int GenerateOrderStatus(std::vector<std::unique_ptr<LockRequest>>& requests);
  int GenerateDelivery(std::vector<std::unique_ptr<LockRequest>>& requests);
  int GenerateStockLevel(std::vector<std::unique_ptr<LockRequest>>& requests);

  int home_warehouse_id_;
  int workload_type_;
  int num_warehouse_;
  int* mix_;
  int* items_;
  unsigned int seed_;
  int tx_type_;
};

}  // namespace proto
}  // namespace rdma

#endif
