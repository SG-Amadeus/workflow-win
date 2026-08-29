#include "timed_connect_op.h"

#include <errno.h>
#include <new>
#include <stdlib.h>

namespace
{

void timed_connect_release(timed_connect_op *self)
{
	composed_op_release(self);
}

void timed_connect_finish(timed_connect_op *self, async_error_code error)
{
	if (!composed_op_try_complete(self))
	{
		timed_connect_release(self);
		return;
	}

	if (self->timer_)
		self->timer_->cancel();
	composed_op_dispatch_result(self, error, 0);
	timed_connect_release(self);
}

} /* namespace */

void timed_connect_op::destroy(composed_op *base)
{
	timed_connect_op *self = static_cast<timed_connect_op *>(base);
	self->~timed_connect_op();
	free(self);
}

void timed_connect_op::complete(composed_op *base)
{
	timed_connect_op *self = static_cast<timed_connect_op *>(base);
		self->callback_(self->context_, self->result_error_);
}

void timed_connect_op::cancel(composed_op *base, cancellation_type type)
{
	timed_connect_op *self = static_cast<timed_connect_op *>(base);
	if (type == cancellation_type::none)
		return;
	self->io_cancel_.emit(type);
	self->timer_->cancel();
}

void timed_connect_op::connect_cb(void *ctx, async_error_code error)
{
	timed_connect_op *self = static_cast<timed_connect_op *>(ctx);
	timed_connect_finish(self, error);
}

void timed_connect_op::timer_cb(void *ctx, async_error_code error)
{
	timed_connect_op *self = static_cast<timed_connect_op *>(ctx);
	if (error)
	{
		timed_connect_release(self);
		return;
	}
	if (!composed_op_try_complete(self))
	{
		timed_connect_release(self);
		return;
	}

	self->io_cancel_.emit(cancellation_type::terminal);
	composed_op_dispatch_result(self, async_socket_error(WSAETIMEDOUT), 0);
	timed_connect_release(self);
}

int timed_connect_start(tcp_socket *socket, steady_timer *timer,
						const struct sockaddr *addr, int addrlen,
						int timeout_ms,
						void (*callback)(void *, async_error_code), void *context,
						cancellation_slot slot, void (*destroy_context)(void *))
{
	if (!socket || !timer || !addr || addrlen <= 0 || !callback)
	{
		errno = EINVAL;
		return -1;
	}

	void *mem = malloc(sizeof(timed_connect_op));
	if (!mem)
	{
		errno = ENOMEM;
		return -1;
	}

	timed_connect_op *op = new (mem) timed_connect_op();
	op->socket_ = socket;
	op->timer_ = timer;
	op->callback_ = callback;
	op->context_ = context;
	composed_op_set_ctx(op, destroy_context, context);
	composed_op_set_executor(op, socket->get_executor());
	composed_op_set_cancellation(op, slot, &timed_connect_op::cancel);

	timer->expires_after(std::chrono::milliseconds(timeout_ms));
	composed_op_add_ref(op);
	if (timer->async_wait(&timed_connect_op::timer_cb, op,
						 composed_op_destroy) != 0)
	{
		int error = errno;
		composed_op_release(op);
		composed_op_release(op);
		errno = error;
		return -1;
	}

	composed_op_add_ref(op);
	if (socket->async_connect(addr, addrlen, &timed_connect_op::connect_cb, op,
						  op->io_cancel_.slot(), composed_op_destroy) != 0)
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

