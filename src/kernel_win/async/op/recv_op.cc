#include "recv_op.h"
#include "async_handler.h"
#include "op_pools.h"
#include "socket_op_common.h"

#include <new>

recv_op *recv_op_alloc(op_pools *pools)
{
	recv_op *op = op_pools_alloc_recv(pools);
	if (op)
	{
		new (op) recv_op();
		op->pools_ = pools;
	}
	return op;
}

void recv_op_free(recv_op *op)
{
	if (!op)
		return;
	op_pools *pools = op->pools_;
	if (op->cancel_token_)
	{
		op->cancel_token_->release();
		op->cancel_token_ = nullptr;
	}
	op_pools_free_recv(pools, op);
}

void recv_op::do_complete(void *owner, win_iocp_operation *base,
						  async_error_code error, size_t bytes)
{
	recv_op *self = static_cast<recv_op *>(base);
	error = socket_op_normalize_error(error, self->cancel_token_);
	if (owner && !error && bytes == 0 && !self->all_empty_)
		/* ASIO maps a zero-byte stream receive to error::eof.  Comm translates
		 * this generic error at its business boundary. */
		error = async_generic_error(async_error_eof);
	async_handler handler = { self->callback_, self->context_, self->destroy_ };
	handler_work completion_work(static_cast<handler_work &&>(self->work_));
	recv_op_free(self);

	if (owner)
		async_handler_dispatch(completion_work, handler, error, bytes);
	else if (handler.destroy)
		handler.destroy(handler.context);
}
