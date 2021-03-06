#include "tpcc_lock_gen.h"

namespace rdma { namespace proto {

TPCCLockGen::TPCCLockGen(int workload_type, int home_warehouse_id, int num_warehouse,
    unsigned int seed, int* mix) {
  workload_type_     = workload_type;
  num_warehouse_     = num_warehouse;
  home_warehouse_id_ = home_warehouse_id;
  mix_               = mix;
  seed_              = seed;

  if (mix_ == NULL) {
    mix_    = new int[5];
    mix_[0] = 45; // NewOrder 45
    mix_[1] = 88; // Payment 43
    mix_[2] = 92; // OrderStatus 4
    mix_[3] = 96; // Delivery 4
    mix_[4] = 100; // StockLevel 4

    //mix_[0] = 100; // NewOrder 45
    //mix_[1] = 51; // Payment 43
    //mix_[2] = 52; // OrderStatus 4
    //mix_[3] = 53; // Delivery 4
    //mix_[4] = 100; // StockLevel 4
  }
  items_ = new int[NUM_ORDER_LINE_PER_ORDER];
}

TPCCLockGen::~TPCCLockGen() {
  delete[] mix_;
}

int TPCCLockGen::Generate(vector<LockRequest*>& requests) {
  int val = 1 + rand_r(&seed_) % 100;
  if (val <= mix_[0]) {
    tx_type_ = NEW_ORDER;
  } else if (val <= mix_[1]) {
    tx_type_ = PAYMENT;
  } else if (val <= mix_[2]) {
    tx_type_ = ORDER_STATUS;
  } else if (val <= mix_[3]) {
    tx_type_ = DELIVERY;
  } else if (val <= mix_[4]) {
    tx_type_ = STOCK_LEVEL;
  } else {
    // ERROR
    return -1;
  }

  switch (tx_type_) {
    case NEW_ORDER:
      return GenerateNewOrder(requests);
      break;
    case PAYMENT:
      return GeneratePayment(requests);
      break;
    case ORDER_STATUS:
      return GenerateOrderStatus(requests);
      break;
    case DELIVERY:
      return GenerateDelivery(requests);
      break;
    case STOCK_LEVEL:
      return GenerateStockLevel(requests);
      break;
    default:
      return -1;
  }

  return -1;
}

int TPCCLockGen::GenerateNewOrder(vector<LockRequest*>& requests) {

  int req_idx = 0;
  int w_id = home_warehouse_id_;

  // "getWarehouseTaxRate": "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?"
  requests[req_idx]->lm_id     = w_id;
  requests[req_idx]->lock_type = SHARED;
  requests[req_idx]->obj_index = WAREHOUSE_START_IDX;
  requests[req_idx]->task      = TASK_LOCK;
  ++req_idx;

  // "getDistrict": "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND D_W_ID = ?"
  // "incrementNextOrderId": "UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ? AND D_W_ID = ?"
  int d_id = rand_r(&seed_) % NUM_ROW_DISTRICT;
  requests[req_idx]->lm_id     = w_id;
  requests[req_idx]->lock_type = EXCLUSIVE;
  requests[req_idx]->obj_index = DISTRICT_START_IDX + d_id;
  requests[req_idx]->task      = TASK_LOCK;
  ++req_idx;

  //  "getCustomer": "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER
  //  WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?"
  int c_d_id = rand_r(&seed_) % NUM_CUSTOMER_PER_DISTRICT;
  int c_id = (d_id * NUM_CUSTOMER_PER_DISTRICT) + c_d_id;
  requests[req_idx]->lm_id     = w_id;
  requests[req_idx]->lock_type = SHARED;
  requests[req_idx]->obj_index = CUSTOMER_START_IDX + c_id;
  requests[req_idx]->task      = TASK_LOCK;
  ++req_idx;

  // "createOrder": "INSERT INTO ORDERS
  // (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL)
  // VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
  int o_id = c_id;
  requests[req_idx]->lm_id     = w_id;
  requests[req_idx]->lock_type = EXCLUSIVE;
  requests[req_idx]->obj_index = ORDER_START_IDX + o_id;
  requests[req_idx]->task      = TASK_LOCK;
  ++req_idx;

  // "createNewOrder": "INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)"
  int n_o_id = o_id;
  requests[req_idx]->lm_id     = w_id;
  requests[req_idx]->lock_type = EXCLUSIVE;
  requests[req_idx]->obj_index = NEW_ORDER_START_IDX + n_o_id;
  requests[req_idx]->task      = TASK_LOCK;
  ++req_idx;

  // "getItemInfo": "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?"
  int i_id = rand_r(&seed_) % NUM_ROW_ITEM;
  requests[req_idx]->lm_id     = w_id;
  requests[req_idx]->lock_type = SHARED;
  requests[req_idx]->obj_index = ITEM_START_IDX + i_id;
  requests[req_idx]->task      = TASK_LOCK;
  ++req_idx;

  // "getStockInfo": "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d
  // FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?"
  // "updateStock": "UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ?
  // WHERE S_I_ID = ? AND S_W_ID = ?"
  int s_id = i_id;
  requests[req_idx]->lm_id     = w_id;
  requests[req_idx]->lock_type = EXCLUSIVE;
  requests[req_idx]->obj_index = STOCK_START_IDX + s_id;
  requests[req_idx]->task      = TASK_LOCK;
  ++req_idx;

  // "createOrderLine": "INSERT INTO ORDER_LINE
  // (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY,
  // OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
  int cnt = 0;
  while (cnt < NUM_ORDER_LINE_PER_ORDER) {
    items_[cnt] = rand_r(&seed_) % NUM_ORDER_LINE_PER_ORDER;
    bool duplicate = false;
    for (int i = 0; i < cnt; ++i) {
      if (items_[cnt] == items_[i]) {
        duplicate = true;
      }
    }
    if (!duplicate) {
      ++cnt;
    }
  }
  for (int i = 0; i < NUM_ORDER_LINE_PER_ORDER; ++i) {
    requests[req_idx]->lm_id     = w_id;
    requests[req_idx]->lock_type = EXCLUSIVE;
    requests[req_idx]->obj_index = ORDER_LINE_START_IDX + (o_id * NUM_ORDER_LINE_PER_ORDER) +
      items_[i];
    requests[req_idx]->task      = TASK_LOCK;
    ++req_idx;
  }

  return req_idx;
}

int TPCCLockGen::GeneratePayment(vector<LockRequest*>& requests) {

  int req_idx = 0;
  int x = 1 + rand_r(&seed_) % 100;
  int y = 1 + rand_r(&seed_) % 100;
  int w_id = home_warehouse_id_;

  int d_id = rand_r(&seed_) % NUM_ROW_DISTRICT;
  //int d_w_id = d_id;
  //int c_d_id;
  int c_w_id;

  if (x <= 85 || num_warehouse_ == 1) {
    //c_d_id = d_id;
    c_w_id = w_id;
  } else {
    //c_d_id = rand_r(&seed_) % NUM_ROW_DISTRICT;
    c_w_id = rand_r(&seed_) % num_warehouse_;
    while (c_w_id == w_id) {
      c_w_id = rand_r(&seed_) % num_warehouse_;
    }
  }
  // "getWarehouse": "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP
  // FROM WAREHOUSE WHERE W_ID = ?"
  // "updateWarehouseBalance": "UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?"
  requests[req_idx]->lm_id     = w_id;
  requests[req_idx]->lock_type = EXCLUSIVE;
  requests[req_idx]->obj_index = WAREHOUSE_START_IDX;
  requests[req_idx]->task      = TASK_LOCK;
  ++req_idx;


  // "getDistrict": "SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT
  // WHERE D_W_ID = ? AND D_ID = ?"
  // "updateDistrictBalance": "UPDATE DISTRICT SET D_YTD = D_YTD + ?
  // WHERE D_W_ID  = ? AND D_ID = ?"
  requests[req_idx]->lm_id     = w_id;
  requests[req_idx]->lock_type = EXCLUSIVE;
  requests[req_idx]->obj_index = DISTRICT_START_IDX + d_id;
  requests[req_idx]->task      = TASK_LOCK;
  ++req_idx;

  int c_id1 = rand_r(&seed_) % NUM_ROW_CUSTOMER;

  if (y <= 60) {
    // "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2,
    // C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE,
    // C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER
    // WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST"
    // "updateBCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?,
    // C_DATA = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?"
    // "updateGCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?
    // WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?"
    int c_id2 = rand_r(&seed_) % NUM_ROW_CUSTOMER;
    while (c_id2 == c_id1) {
      c_id2 = rand_r(&seed_) % NUM_ROW_CUSTOMER;
    }
    requests[req_idx]->lm_id     = c_w_id;
    requests[req_idx]->lock_type = EXCLUSIVE;
    requests[req_idx]->obj_index = CUSTOMER_START_IDX + c_id1;
    requests[req_idx]->task      = TASK_LOCK;
    ++req_idx;

    requests[req_idx]->lm_id     = c_w_id;
    requests[req_idx]->lock_type = EXCLUSIVE;
    requests[req_idx]->obj_index = CUSTOMER_START_IDX + c_id2;
    requests[req_idx]->task      = TASK_LOCK;
    ++req_idx;

    // "insertHistory": "INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    requests[req_idx]->lm_id     = w_id;
    requests[req_idx]->lock_type = EXCLUSIVE;
    requests[req_idx]->obj_index = HISTORY_START_IDX + c_id1;
    requests[req_idx]->task      = TASK_LOCK;
    ++req_idx;

    requests[req_idx]->lm_id     = w_id;
    requests[req_idx]->lock_type = EXCLUSIVE;
    requests[req_idx]->obj_index = HISTORY_START_IDX + c_id2;
    requests[req_idx]->task      = TASK_LOCK;
    ++req_idx;
  } else {
    // "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2,
    // C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE,
    // C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER
    // WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?"
    // "updateBCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?,
    // C_DATA = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?"
    // "updateGCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?
    // WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?"
    requests[req_idx]->lm_id     = c_w_id;
    requests[req_idx]->lock_type = EXCLUSIVE;
    requests[req_idx]->obj_index = CUSTOMER_START_IDX + c_id1;
    requests[req_idx]->task      = TASK_LOCK;
    ++req_idx;

    // "insertHistory": "INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    requests[req_idx]->lm_id     = w_id;
    requests[req_idx]->lock_type = EXCLUSIVE;
    requests[req_idx]->obj_index = HISTORY_START_IDX + c_id1;
    requests[req_idx]->task      = TASK_LOCK;
    ++req_idx;
  }

  return req_idx;
}

int TPCCLockGen::GenerateOrderStatus(vector<LockRequest*>& requests) {

  int req_idx = 0;
  int y = 1 + rand_r(&seed_) % 100;
  int w_id = home_warehouse_id_;
  int c_id = rand_r(&seed_) % NUM_ROW_CUSTOMER;
  int o_id = c_id;

  if (y <= 40) {
  // "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER
  // WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST"
    int c_id2= rand_r(&seed_) % NUM_ROW_CUSTOMER;
    while (c_id2 == c_id) {
      c_id2= rand_r(&seed_) % NUM_ROW_CUSTOMER;
    }
    requests[req_idx]->lm_id     = w_id;
    requests[req_idx]->lock_type = SHARED;
    requests[req_idx]->obj_index = CUSTOMER_START_IDX + c_id;
    requests[req_idx]->task      = TASK_LOCK;
    ++req_idx;

    requests[req_idx]->lm_id     = w_id;
    requests[req_idx]->lock_type = SHARED;
    requests[req_idx]->obj_index = CUSTOMER_START_IDX + c_id2;
    requests[req_idx]->task      = TASK_LOCK;
    ++req_idx;

    // "getLastOrder": "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS
    // WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1"
    requests[req_idx]->lm_id     = w_id;
    requests[req_idx]->lock_type = SHARED;
    requests[req_idx]->obj_index = ORDER_START_IDX + c_id;
    requests[req_idx]->task      = TASK_LOCK;
    ++req_idx;

    requests[req_idx]->lm_id     = w_id;
    requests[req_idx]->lock_type = SHARED;
    requests[req_idx]->obj_index = ORDER_START_IDX + c_id2;
    requests[req_idx]->task      = TASK_LOCK;
    ++req_idx;
  } else {
    // "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER
    // WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?"
    requests[req_idx]->lm_id     = w_id;
    requests[req_idx]->lock_type = SHARED;
    requests[req_idx]->obj_index = CUSTOMER_START_IDX + c_id;
    requests[req_idx]->task      = TASK_LOCK;
    ++req_idx;

    // "getLastOrder": "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS
    // WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1"
    requests[req_idx]->lm_id     = w_id;
    requests[req_idx]->lock_type = SHARED;
    requests[req_idx]->obj_index = ORDER_START_IDX + c_id;
    requests[req_idx]->task      = TASK_LOCK;
    ++req_idx;
  }

  // "getOrderLines": "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D
  // FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?"
  int cnt = 0;
  while (cnt < NUM_ORDER_LINE_PER_ORDER) {
    items_[cnt] = rand_r(&seed_) % NUM_ORDER_LINE_PER_ORDER;
    bool duplicate = false;
    for (int i = 0; i < cnt; ++i) {
      if (items_[cnt] == items_[i]) {
        duplicate = true;
      }
    }
    if (!duplicate) {
      ++cnt;
    }
  }
  for (int i = 0 ; i < NUM_ORDER_LINE_PER_ORDER; ++i) {
    requests[req_idx]->lm_id     = w_id;
    requests[req_idx]->lock_type = SHARED;
    requests[req_idx]->obj_index = ORDER_LINE_START_IDX +
      (o_id * NUM_ORDER_LINE_PER_ORDER) + items_[i];
    requests[req_idx]->task      = TASK_LOCK;
    ++req_idx;
  }

  return req_idx;
}

int TPCCLockGen::GenerateDelivery(vector<LockRequest*>& requests) {

  int req_idx = 0;

  // "getNewOrder": "SELECT NO_O_ID FROM NEW_ORDER
  // WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1"
  // "deleteNewOrder": "DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID = ?"
  int w_id = home_warehouse_id_;
  int d_id = rand_r(&seed_) % NUM_ROW_DISTRICT;
  int c_d_id = rand_r(&seed_) % NUM_CUSTOMER_PER_DISTRICT;
  int n_o_id = (d_id * NUM_CUSTOMER_PER_DISTRICT) + c_d_id;
  requests[req_idx]->lm_id     = w_id;
  requests[req_idx]->lock_type = EXCLUSIVE;
  requests[req_idx]->obj_index = NEW_ORDER_START_IDX + n_o_id;
  requests[req_idx]->task      = TASK_LOCK;
  ++req_idx;

  // "getCId": "SELECT O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?"
  // "updateOrders": "UPDATE ORDERS SET O_CARRIER_ID = ?
  // WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?"
  int o_id = n_o_id;
  requests[req_idx]->lm_id     = w_id;
  requests[req_idx]->lock_type = EXCLUSIVE;
  requests[req_idx]->obj_index = ORDER_START_IDX + o_id;
  requests[req_idx]->task      = TASK_LOCK;
  ++req_idx;

  // "updateOrderLine": "UPDATE ORDER_LINE SET OL_DELIVERY_D = ?
  // WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?"
  // "sumOLAmount": "SELECT SUM(OL_AMOUNT) FROM ORDER_LINE
  // WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?"
  int cnt = 0;
  while (cnt < NUM_ORDER_LINE_PER_ORDER) {
    items_[cnt] = rand_r(&seed_) % NUM_ORDER_LINE_PER_ORDER;
    bool duplicate = false;
    for (int i = 0; i < cnt; ++i) {
      if (items_[cnt] == items_[i]) {
        duplicate = true;
      }
    }
    if (!duplicate) {
      ++cnt;
    }
  }
  for (int i = 0 ; i < NUM_ORDER_LINE_PER_ORDER; ++i) {
    requests[req_idx]->lm_id     = w_id;
    requests[req_idx]->lock_type = EXCLUSIVE;
    requests[req_idx]->obj_index = ORDER_LINE_START_IDX +
      (o_id * NUM_ORDER_LINE_PER_ORDER) + items_[i];
    requests[req_idx]->task      = TASK_LOCK;
    ++req_idx;
  }

  // "updateCustomer": "UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ?
  // WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?"
  int c_id = n_o_id;
  requests[req_idx]->lm_id     = w_id;
  requests[req_idx]->lock_type = EXCLUSIVE;
  requests[req_idx]->obj_index = CUSTOMER_START_IDX + c_id;
  requests[req_idx]->task      = TASK_LOCK;
  ++req_idx;

  return req_idx;
}

int TPCCLockGen::GenerateStockLevel(vector<LockRequest*>& requests) {

  int req_idx = 0;

  // "getOId": "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?"
  int w_id = home_warehouse_id_;
  int d_id = rand_r(&seed_) % NUM_ROW_DISTRICT;
  requests[req_idx]->lm_id     = w_id;
  requests[req_idx]->lock_type = SHARED;
  requests[req_idx]->obj_index = DISTRICT_START_IDX + d_id;
  requests[req_idx]->task      = TASK_LOCK;
  ++req_idx;

  // "getStockCount": """
  //   SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK
  //    WHERE OL_W_ID = ?
  //    AND OL_D_ID = ?
  //    AND OL_O_ID < ?
  //    AND OL_O_ID >= ?
  //    AND S_W_ID = ?
  //    AND S_I_ID = OL_I_ID
  //    AND S_QUANTITY < ?
  //   """
  int o_id = rand_r(&seed_) % NUM_ROW_ORDER;
  //int s_id = rand_r(&seed_) % NUM_ROW_STOCK;
  int cnt = 0;
  while (cnt < NUM_ORDER_LINE_PER_ORDER) {
    items_[cnt] = rand_r(&seed_) % NUM_ORDER_LINE_PER_ORDER;
    bool duplicate = false;
    for (int i = 0; i < cnt; ++i) {
      if (items_[cnt] == items_[i]) {
        duplicate = true;
      }
    }
    if (!duplicate) {
      ++cnt;
    }
  }
  for (int i = 0 ; i < NUM_ORDER_LINE_PER_ORDER; ++i) {
    requests[req_idx]->lm_id     = w_id;
    requests[req_idx]->lock_type = SHARED;
    requests[req_idx]->obj_index = ORDER_LINE_START_IDX +
      (o_id * NUM_ORDER_LINE_PER_ORDER) + items_[i];
    requests[req_idx]->task      = TASK_LOCK;
    ++req_idx;
  }
  cnt = 0;
  while (cnt < NUM_ORDER_LINE_PER_ORDER) {
    items_[cnt] = rand_r(&seed_) % NUM_ROW_STOCK;
    bool duplicate = false;
    for (int i = 0; i < cnt; ++i) {
      if (items_[cnt] == items_[i]) {
        duplicate = true;
      }
    }
    if (!duplicate) {
      ++cnt;
    }
  }
  for (int i = 0 ; i < NUM_ORDER_LINE_PER_ORDER; ++i) {
    requests[req_idx]->lm_id     = w_id;
    requests[req_idx]->lock_type = SHARED;
    requests[req_idx]->obj_index = STOCK_START_IDX + items_[i];
    requests[req_idx]->task      = TASK_LOCK;
    ++req_idx;
  }

  return req_idx;
}

}}
