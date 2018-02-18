#ifndef RDMA_PROTO_TPCCLOCKGEN_H
#define RDMA_PROTO_TPCCLOCKGEN_H

#include <stdlib.h>
#include <sys/types.h>
#include <cstddef>
#include <ctime>
#include <iostream>
#include <memory>
#include <random>
#include <thread>
#include <vector>

#include "constants.h"
#include "lock_request.h"

#include "Poco/Random.h"

using namespace std;

namespace rdma {
namespace proto {

class TPCCLockGen {
 public:
  TPCCLockGen(int home_warehouse_id, int num_warehouse,
              double full_scan_ratio = 0, int full_scan_rows = 0,
              int full_scan_time = 0);
  ~TPCCLockGen();
  int Generate(std::vector<std::unique_ptr<LockRequest>>& requests);

  static const int NEW_ORDER = 1;
  static const int PAYMENT = 2;
  static const int ORDER_STATUS = 3;
  static const int DELIVERY = 4;
  static const int STOCK_LEVEL = 5;
  static const int SCAN_CUSTOMER = 6;

  static const int TABLE_WAREHOUSE = 0;
  static const int TABLE_DISTRICT = 1;
  static const int TABLE_CUSTOMER = 2;
  static const int TABLE_ORDER = 3;
  static const int TABLE_HISTORY = 4;
  static const int TABLE_NEWORDER = 5;
  static const int TABLE_ORDERLINE = 6;
  static const int TABLE_STOCK = 7;
  static const int TABLE_ITEM = 8;

  static const int WAREHOUSE_START_IDX = 10;
  static const int DISTRICT_START_IDX = 1 + 10;
  static const int CUSTOMER_START_IDX = 11 + 10;
  static const int ORDER_START_IDX = 30011 + 10;
  static const int HISTORY_START_IDX = 60011 + 10;
  static const int NEW_ORDER_START_IDX = 90011 + 10;
  static const int ORDER_LINE_START_IDX = 120011 + 10;
  static const int STOCK_START_IDX = 420011 + 10;
  static const int ITEM_START_IDX = 520011 + 10;

  static const int NUM_ROW_WAREHOUSE = 1;
  static const int NUM_ROW_DISTRICT = 10;
  static const int NUM_ROW_CUSTOMER = 30000;
  static const int NUM_ROW_ORDER = 30000;
  static const int NUM_ROW_HISTORY = 30000;
  static const int NUM_ROW_NEW_ORDER = 30000;
  static const int NUM_ROW_ORDER_LINE = 300000;
  static const int NUM_ROW_STOCK = 100000;
  static const int NUM_ROW_ITEM = 100000;

  static const int NUM_ORDER_LINE_PER_ORDER = 1;
  static const int NUM_ORDER_LINE_PER_STOCK = 3;
  static const int NUM_HISTORY_PER_CUSTOMER = 1;
  static const int NUM_CUSTOMER_PER_DISTRICT = 3000;

 private:
  int GenerateScanNewOrder(std::vector<std::unique_ptr<LockRequest>>& requests);
  int GenerateScanCustomer(std::vector<std::unique_ptr<LockRequest>>& requests);
  int GenerateNewOrder(std::vector<std::unique_ptr<LockRequest>>& requests);
  int GeneratePayment(std::vector<std::unique_ptr<LockRequest>>& requests);
  int GenerateOrderStatus(std::vector<std::unique_ptr<LockRequest>>& requests);
  int GenerateDelivery(std::vector<std::unique_ptr<LockRequest>>& requests);
  int GenerateStockLevel(std::vector<std::unique_ptr<LockRequest>>& requests);

  int home_warehouse_id_;
  int workload_type_;
  int num_warehouse_;
  double full_scan_ratio_;
  int full_scan_rows_;
  int full_scan_time_;
  int* mix_;
  int* items_;
  unsigned int seed_;
  int tx_type_;
  Poco::Random rng_;
  std::default_random_engine generator_;
  std::normal_distribution<double>* normal_dist_;
  std::exponential_distribution<double>* exp_dist_;
};

}  // namespace proto
}  // namespace rdma

#endif
