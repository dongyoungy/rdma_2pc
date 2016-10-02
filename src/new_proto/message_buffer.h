#ifndef RDMA_PROTO_MESSAGE_BUFFER_H
#define RDMA_PROTO_MESSAGE_BUFFER_H

#include <iostream>
#include <vector>
#include "message.h"
#include "constants.h"
#include "context.h"

using namespace std;

namespace rdma { namespace proto {

class Context;

// Ring message buffer
class MessageBuffer {
  public:
    MessageBuffer();
    ~MessageBuffer();
    int Register(Context* context);
    Message* GetMessage();
    struct ibv_mr* GetMR();
    void Rotate();

  private:
    int index_;
    int size_;
    vector<Message*> messages_;
    vector<ibv_mr*> mrs_;
};

}}

#endif
