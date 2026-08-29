#include "accept_op.h"
#include "op_pools.h"
#include "socket_op_common.h"

#include <errno.h>
#include <new>
#include <stdlib.h>

namespace
{

struct accept_call
{
	void (*callback)(void *, async_error_code, SOCKET);
	void (*destroy)(void *);
	void *context;
	async_error_code error;
	SOCKET socket;
};

void accept_call_routine(void *arg)
{
	accept_call *call = static_cast<accept_call *>(arg);
	call->callback(call->context, call->error, call->socket);
	free(call);
}

void accept_call_destroy(void *arg)
{
	accept_call *call = static_cast<accept_call *>(arg);
	if (call->socket != INVALID_SOCKET)
		::closesocket(call->socket);
	if (call->destroy)
		call->destroy(call->context);
	free(call);
}

} /* namespace */

accept_op *accept_op_alloc(op_pools *pools)
{
	accept_op *op = op_pools_alloc_accept(pools);
	if (op)
	{
		new (op) accept_op();
		op->pools_ = pools;
	}
	return op;
}

void accept_op_free(accept_op *op)
{
	if (!op)
		return;
	op_pools *pools = op->pools_;
	if (op->cancel_token_)
	{
		op->cancel_token_->release();
		op->cancel_token_ = nullptr;
	}
	op_pools_free_accept(pools, op);
}

void accept_op::do_complete(void *owner, win_iocp_operation *base,
							 async_error_code error, size_t /*bytes*/)
{
	accept_op *self = static_cast<accept_op *>(base);
	if (error.value() == ERROR_NETNAME_DELETED)
	{
		if ((self->cancel_token_ && self->cancel_token_->is_closed()) ||
			self->cancelled_)
			error = async_system_error(ERROR_OPERATION_ABORTED);
		else
			/* ASIO complete_iocp_accept maps this to connection_aborted. */
			error = async_socket_error(WSAECONNABORTED);
	}
	else
		error = socket_op_normalize_error(error, self->cancel_token_);

	if (owner && !error && ::setsockopt(
			self->accepted_, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT,
			(const char *)&self->listener_, sizeof(self->listener_)) != 0)
		error = async_socket_error(::WSAGetLastError());

	void (*callback)(void *, async_error_code, SOCKET) = self->callback_;
	void (*destroy)(void *) = self->destroy_;
	void *context = self->context_;
	SOCKET accepted = !error ? self->accepted_ : INVALID_SOCKET;
	self->accepted_ = INVALID_SOCKET;
	handler_work completion_work(static_cast<handler_work &&>(self->work_));
	accept_op_free(self);

	if (!owner)
	{
		if (accepted != INVALID_SOCKET)
			::closesocket(accepted);
		if (destroy)
			destroy(context);
		return;
	}

	accept_call *call = static_cast<accept_call *>(malloc(sizeof *call));
	if (!call)
	{
		if (accepted != INVALID_SOCKET)
			::closesocket(accepted);
		if (destroy)
			destroy(context);
		return;
	}
	call->callback = callback;
	call->destroy = destroy;
	call->context = context;
	call->error = error;
	call->socket = accepted;
	if (completion_work.complete(&accept_call_routine, call,
								 &accept_call_destroy) != 0)
	{
		free(call);
		if (accepted != INVALID_SOCKET)
			::closesocket(accepted);
		if (destroy)
			destroy(context);
	}
}
