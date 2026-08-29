/*
  AsyncCore: ASIO win_iocp_socket_recv_op, non-template.
*/

#ifndef _ASYNC_OP_RECV_OP_H_
#define _ASYNC_OP_RECV_OP_H_

#include "win_iocp_operation.h"
#include "../service/handler_work.h"
#include "../service/cancel_token.h"

class op_pools;

class recv_op : public win_iocp_operation
{
public:
	op_pools *pools_;
	handler_work work_;
	cancel_token *cancel_token_;
	void (*callback_)(void *, async_error_code, size_t);
	void *context_;
	void (*destroy_)(void *);
	bool all_empty_;

	recv_op() : win_iocp_operation(&recv_op::do_complete),
		cancel_token_(nullptr), all_empty_(false) {}

	static void do_complete(void *owner, win_iocp_operation *base,
							async_error_code error, size_t bytes);
};

recv_op *recv_op_alloc(op_pools *pools);
void recv_op_free(recv_op *op);

#endif /* _ASYNC_OP_RECV_OP_H_ */

