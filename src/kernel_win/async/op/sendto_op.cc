#include "sendto_op.h"
#include "async_handler.h"
#include "op_pools.h"
#include "socket_op_common.h"

#include <new>

sendto_op *sendto_op_alloc(op_pools *pools)
{
	sendto_op *op = op_pools_alloc_sendto(pools);
	if (op)
	{
		new (op) sendto_op();
		op->pools_ = pools;
	}
	return op;
}

void sendto_op_free(sendto_op *op)
{
	if (!op)
		return;
	op_pools *pools = op->pools_;
	if (op->cancel_token_)
	{
		op->cancel_token_->release();
		op->cancel_token_ = nullptr;
	}
	op_pools_free_sendto(pools, op);
}

void sendto_op::do_complete(void *owner, win_iocp_operation *base,
							 async_error_code error, size_t bytes)
{
	sendto_op *self = static_cast<sendto_op *>(base);
	error = socket_op_normalize_error(error, self->cancel_token_);
	async_handler handler = { self->callback_, self->context_, self->destroy_ };
	handler_work completion_work(static_cast<handler_work &&>(self->work_));
	sendto_op_free(self);

	if (owner)
		async_handler_dispatch(completion_work, handler, error, bytes);
	else if (handler.destroy)
		handler.destroy(handler.context);
}
