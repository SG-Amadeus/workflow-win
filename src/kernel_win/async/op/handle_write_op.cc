#include "handle_write_op.h"
#include "async_handler.h"
#include "op_pools.h"
#include "../error.h"

#include <new>

handle_write_op *handle_write_op_alloc(op_pools *pools)
{
	handle_write_op *op = op_pools_alloc_handle_write(pools);
	if (op)
	{
		new (op) handle_write_op();
		op->pools_ = pools;
	}
	return op;
}

void handle_write_op_free(handle_write_op *op)
{
	if (op)
		op_pools_free_handle_write(op->pools_, op);
}

void handle_write_op::do_complete(void *owner, win_iocp_operation *base,
							  async_error_code error, size_t bytes)
{
	handle_write_op *self = static_cast<handle_write_op *>(base);
	if (error.value() == ERROR_MORE_DATA)
		error.clear();
	async_handler handler = { self->callback_, self->context_, self->destroy_ };
	handler_work completion_work(static_cast<handler_work &&>(self->work_));
	handle_write_op_free(self);

	if (owner)
		async_handler_dispatch(completion_work, handler, error, bytes);
	else if (handler.destroy)
		handler.destroy(handler.context);
}
