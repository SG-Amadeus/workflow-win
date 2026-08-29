#include "timed_accept_op.h"

#include <errno.h>
#include <new>
#include <stdlib.h>

namespace
{

void timed_accept_release(timed_accept_op *self)
{
	composed_op_release(self);
}

void timed_accept_finish(timed_accept_op *self, async_error_code error, SOCKET socket)
{
	if (!composed_op_try_complete(self))
	{
		timed_accept_release(self);
		return;
	}

	if (self->timer_)
		self->timer_->cancel();
	self->result_socket_ = (UINT_PTR)socket;
	composed_op_dispatch_result(self, error, 0);
	timed_accept_release(self);
}

} /* namespace */

void timed_accept_op::destroy(composed_op *base)
{
	timed_accept_op *self = static_cast<timed_accept_op *>(base);
	self->~timed_accept_op();
	free(self);
}

void timed_accept_op::destroy_cb(void *ctx)
{
	timed_accept_op *self = static_cast<timed_accept_op *>(ctx);
	if (self->destroy_)
		self->destroy_(self->context_);
	composed_op_abandon(self);
}

void timed_accept_op::complete(composed_op *base)
{
	timed_accept_op *self = static_cast<timed_accept_op *>(base);
		self->callback_(self->context_, self->result_error_,
					(SOCKET)self->result_socket_);
}

void timed_accept_op::cancel(composed_op *base, cancellation_type type)
{
	timed_accept_op *self = static_cast<timed_accept_op *>(base);
	if (type == cancellation_type::none)
		return;
	self->io_cancel_.emit(type);
	self->timer_->cancel();
}

void timed_accept_op::accept_cb(void *ctx, async_error_code error, SOCKET socket)
{
	timed_accept_op *self = static_cast<timed_accept_op *>(ctx);
	timed_accept_finish(self, error, socket);
}

void timed_accept_op::timer_cb(void *ctx, async_error_code error)
{
	timed_accept_op *self = static_cast<timed_accept_op *>(ctx);
	if (error)
	{
		timed_accept_release(self);
		return;
	}
	if (!composed_op_try_complete(self))
	{
		timed_accept_release(self);
		return;
	}

	self->io_cancel_.emit(cancellation_type::terminal);
	self->result_socket_ = (UINT_PTR)INVALID_SOCKET;
	composed_op_dispatch_result(self, async_socket_error(WSAETIMEDOUT), 0);
	timed_accept_release(self);
}

int timed_accept_start(tcp_acceptor *acceptor, steady_timer *timer,
					   int timeout_ms,
					   void (*destroy)(void *),
					   void (*callback)(void *, async_error_code, SOCKET),
					   void *context, cancellation_slot slot,
					   void (*destroy_context)(void *))
{
	if (!acceptor || !timer || !callback)
	{
		errno = EINVAL;
		return -1;
	}

	void *mem = malloc(sizeof(timed_accept_op));
	if (!mem)
	{
		errno = ENOMEM;
		return -1;
	}

	timed_accept_op *op = new (mem) timed_accept_op();
	op->acceptor_ = acceptor;
	op->timer_ = timer;
	op->destroy_ = destroy;
	op->callback_ = callback;
	op->context_ = context;
	composed_op_set_ctx(op, destroy_context, context);
	composed_op_set_executor(op, acceptor->get_executor());
	composed_op_set_cancellation(op, slot, &timed_accept_op::cancel);

	timer->expires_after(std::chrono::milliseconds(timeout_ms));
	composed_op_add_ref(op);
	if (timer->async_wait(&timed_accept_op::timer_cb, op,
						 composed_op_destroy) != 0)
	{
		int error = errno;
		composed_op_release(op);
		composed_op_release(op);
		errno = error;
		return -1;
	}

	composed_op_add_ref(op);
	if (acceptor->async_accept(&timed_accept_op::accept_cb, op,
								op->io_cancel_.slot(),
								&timed_accept_op::destroy_cb) != 0)
	{
		async_error_code error = async_error_from_errno(errno);
		composed_op_release(op);
		composed_op_complete_with_error(op, error, 0);
		composed_op_release(op);
		return 0;
	}

	composed_op_release(op);
	return 0;
}

