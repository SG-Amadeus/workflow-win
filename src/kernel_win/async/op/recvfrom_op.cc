#include "recvfrom_op.h"
#include "async_handler.h"
#include "op_pools.h"
#include "socket_op_common.h"

#include <new>

recvfrom_op *recvfrom_op_alloc(op_pools *pools)
{
	recvfrom_op *op = op_pools_alloc_recvfrom(pools);
	if (op)
	{
		new (op) recvfrom_op();
		op->pools_ = pools;
	}
	return op;
}

void recvfrom_op_free(recvfrom_op *op)
{
	if (!op)
		return;
	op_pools *pools = op->pools_;
	if (op->cancel_token_)
	{
		op->cancel_token_->release();
		op->cancel_token_ = nullptr;
	}
	op_pools_free_recvfrom(pools, op);
}

void recvfrom_op::do_complete(void *owner, win_iocp_operation *base,
							  async_error_code error, size_t bytes)
{
	recvfrom_op *self = static_cast<recvfrom_op *>(base);
	error = socket_op_normalize_error(error, self->cancel_token_);
	if (error.value() == WSAEMSGSIZE || error.value() == ERROR_MORE_DATA)
		error.clear();
	async_handler handler = { self->callback_, self->context_, self->destroy_ };
	handler_work completion_work(static_cast<handler_work &&>(self->work_));
	recvfrom_op_free(self);

	if (owner)
		async_handler_dispatch(completion_work, handler, error, bytes);
	else if (handler.destroy)
		handler.destroy(handler.context);
}
