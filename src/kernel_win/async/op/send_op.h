/*
  AsyncCore: ASIO win_iocp_socket_send_op, non-template.
*/

#ifndef _ASYNC_OP_SEND_OP_H_
#define _ASYNC_OP_SEND_OP_H_

#include "win_iocp_operation.h"
#include "../service/handler_work.h"
#include "../service/cancel_token.h"

class op_pools;

class send_op : public win_iocp_operation
{
public:
	op_pools *pools_;
	handler_work work_;
	cancel_token *cancel_token_;
	void (*callback_)(void *, async_error_code, size_t);
	void *context_;
	void (*destroy_)(void *);

	send_op() : win_iocp_operation(&send_op::do_complete),
		cancel_token_(nullptr) {}

	static void do_complete(void *owner, win_iocp_operation *base,
							async_error_code error, size_t bytes);
};

send_op *send_op_alloc(op_pools *pools);
void send_op_free(send_op *op);

#endif /* _ASYNC_OP_SEND_OP_H_ */

