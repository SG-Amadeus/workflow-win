/*
  AsyncCore: connect operation for tcp_socket.
*/

#ifndef _ASYNC_OP_CONNECT_OP_H_
#define _ASYNC_OP_CONNECT_OP_H_

#include "win_iocp_operation.h"
#include "../service/handler_work.h"
#include "../service/cancel_token.h"

class op_pools;

class connect_op : public win_iocp_operation
{
public:
	op_pools *pools_;
	handler_work work_;
	SOCKET socket_;
	cancel_token *cancel_token_;
	void (*callback_)(void *, async_error_code);
	void *context_;
	void (*destroy_)(void *);

	connect_op() : win_iocp_operation(&connect_op::do_complete),
		socket_(INVALID_SOCKET), cancel_token_(nullptr) {}

	static void do_complete(void *owner, win_iocp_operation *base,
							async_error_code error, size_t bytes);
};

connect_op *connect_op_alloc(op_pools *pools);
void connect_op_free(connect_op *op);

#endif /* _ASYNC_OP_CONNECT_OP_H_ */

