#include "connect_op.h"
#include "op_pools.h"
#include "socket_op_common.h"

#include <errno.h>
#include <new>
#include <stdlib.h>

namespace
{

struct connect_call
{
	void (*callback)(void *, async_error_code);
	void (*destroy)(void *);
	void *context;
	async_error_code error;
};

void connect_call_routine(void *arg)
{
	connect_call *call = static_cast<connect_call *>(arg);
	call->callback(call->context, call->error);
	free(call);
}

void connect_call_destroy(void *arg)
{
	connect_call *call = static_cast<connect_call *>(arg);
	if (call->destroy)
		call->destroy(call->context);
	free(call);
}

} /* namespace */

connect_op *connect_op_alloc(op_pools *pools)
{
	connect_op *op = op_pools_alloc_connect(pools);
	if (op)
	{
		new (op) connect_op();
		op->pools_ = pools;
	}
	return op;
}

void connect_op_free(connect_op *op)
{
	if (!op)
		return;
	op_pools *pools = op->pools_;
	if (op->cancel_token_)
	{
		op->cancel_token_->release();
		op->cancel_token_ = nullptr;
	}
	op_pools_free_connect(pools, op);
}

void connect_op::do_complete(void *owner, win_iocp_operation *base,
							 async_error_code error, size_t /*bytes*/)
{
	connect_op *self = static_cast<connect_op *>(base);
	error = socket_op_normalize_error(error, self->cancel_token_);

	if (owner && !error && ::setsockopt(
		self->socket_, SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT,
		nullptr, 0) != 0)
		error = async_socket_error(::WSAGetLastError());

	void (*callback)(void *, async_error_code) = self->callback_;
	void (*destroy)(void *) = self->destroy_;
	void *context = self->context_;
	handler_work completion_work(static_cast<handler_work &&>(self->work_));
	connect_op_free(self);

	if (!owner)
	{
		if (destroy)
			destroy(context);
		return;
	}

	connect_call *call = static_cast<connect_call *>(malloc(sizeof *call));
	if (!call)
	{
		if (destroy)
			destroy(context);
		return;
	}
	call->callback = callback;
	call->destroy = destroy;
	call->context = context;
	call->error = error;
	if (completion_work.complete(&connect_call_routine, call,
								 &connect_call_destroy) != 0)
	{
		free(call);
		if (destroy)
			destroy(context);
	}
}
