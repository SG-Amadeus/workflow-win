#include "handle_read_op.h"
#include "async_handler.h"
#include "op_pools.h"
#include "../error.h"

#include <new>

handle_read_op *handle_read_op_alloc(op_pools *pools)
{
	handle_read_op *op = op_pools_alloc_handle_read(pools);
	if (op)
	{
		new (op) handle_read_op();
		op->pools_ = pools;
	}
	return op;
}

void handle_read_op_free(handle_read_op *op)
{
	if (op)
		op_pools_free_handle_read(op->pools_, op);
}

void handle_read_op::do_complete(void *owner, win_iocp_operation *base,
							 async_error_code error, size_t bytes)
{
	handle_read_op *self = static_cast<handle_read_op *>(base);
	if (error.value() == ERROR_MORE_DATA)
		error.clear();
	async_handler handler = { self->callback_, self->context_, self->destroy_ };
	handler_work completion_work(static_cast<handler_work &&>(self->work_));
	handle_read_op_free(self);

	if (owner)
		async_handler_dispatch(completion_work, handler, error, bytes);
	else if (handler.destroy)
		handler.destroy(handler.context);
}
