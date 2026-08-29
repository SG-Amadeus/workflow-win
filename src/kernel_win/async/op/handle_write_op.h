/*
  AsyncCore: ASIO win_iocp_handle_write_op, non-template single-buffer write.
*/

#ifndef _ASYNC_OP_HANDLE_WRITE_OP_H_
#define _ASYNC_OP_HANDLE_WRITE_OP_H_

#include "win_iocp_operation.h"
#include "../service/handler_work.h"

class op_pools;

class handle_write_op : public win_iocp_operation
{
public:
	op_pools *pools_;
	handler_work work_;
	void (*callback_)(void *, async_error_code, size_t);
	void *context_;
	void (*destroy_)(void *);

	handle_write_op() : win_iocp_operation(&handle_write_op::do_complete) {}

	static void do_complete(void *owner, win_iocp_operation *base,
							async_error_code error, size_t bytes);
};

handle_write_op *handle_write_op_alloc(op_pools *pools);
void handle_write_op_free(handle_write_op *op);

#endif /* _ASYNC_OP_HANDLE_WRITE_OP_H_ */

