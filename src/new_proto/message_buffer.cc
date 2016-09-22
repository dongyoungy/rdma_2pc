#include "message_buffer.h"

namespace rdma { namespace proto {

MessageBuffer::MessageBuffer() {
  index_ = 0;
  for (int i = 0; i < MAX_MESSAGE_BUFFER_SIZE; ++i) {
    Message* message = new Message;
    messages_.push_back(message);
  }
  size_ = messages_.size();
}

MessageBuffer::~MessageBuffer() {
  for (int i = 0; i < messages_.size(); ++i) {
    ibv_dereg_mr(messages_[i]->mr);
    delete messages_[i];
  }
  messages_.clear();
}

int MessageBuffer::Register(Context* context) {
  for (int i = 0; i < size_; ++i) {
    Message* message = messages_[i];
    message->mr = ibv_reg_mr(context->protection_domain,
        message,
        sizeof(*message),
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
        IBV_ACCESS_REMOTE_ATOMIC);
    if (message->mr == NULL) {
      cerr << "MessageBuffer::Register(): ibv_reg_mr() failed." << endl;
      return -1;
    }
  }
  return 0;
}

Message* MessageBuffer::GetMessage() {
  return messages_[index_];
}

void MessageBuffer::Rotate() {
  index_ = (index_ + 1) % size_;
}

}}
