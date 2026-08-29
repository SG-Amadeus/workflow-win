#include "handle_readv_op.h"
#include "async_handler.h"
#include "op_pools.h"
#include "../error.h"

#include <new>
#include <string.h>

handle_readv_op *handle_readv_op_alloc(op_pools *pools)
{
	handle_readv_op *op = op_pools_alloc_handle_readv(pools);
	if (op)
	{
		new (op) handle_readv_op();
		op->pools_ = pools;
	}
	return op;
}

void handle_readv_op_free(handle_readv_op *op)
{
	if (op)
		op_pools_free_handle_readv(op->pools_, op);
}

void handle_readv_op::do_complete(void *owner, win_iocp_operation *base,
							  async_error_code error, size_t bytes)
{
	handle_readv_op *self = static_cast<handle_readv_op *>(base);
	if (error.value() == ERROR_MORE_DATA)
		error.clear();
	if (owner && !error)
	{
		size_t copied = 0;
		for (int i = 0; i < self->iov_count_; ++i)
		{
			size_t n = self->iov_[i].iov_len;
			if (copied + n > bytes)
				n = bytes - copied;
			if (n == 0)
				break;
			memcpy(self->iov_[i].iov_base, self->temp_ + copied, n);
			copied += n;
		}
	}
	async_handler handler = { self->callback_, self->context_, self->destroy_ };
	handler_work completion_work(static_cast<handler_work &&>(self->work_));
	handle_readv_op_free(self);

	if (owner)
		async_handler_dispatch(completion_work, handler, error, bytes);
	else if (handler.destroy)
		handler.destroy(handler.context);
}
