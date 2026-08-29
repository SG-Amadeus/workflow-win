#include "timed_handshake_op.h"

#include <errno.h>
#include <new>
#include <stdlib.h>

namespace
{

void timed_handshake_release(timed_handshake_op *self)
{
	composed_op_release(self);
}

void timed_handshake_finish(timed_handshake_op *self, async_error_code error)
{
	if (!composed_op_try_complete(self))
	{
		timed_handshake_release(self);
		return;
	}

	if (self->timer_)
		self->timer_->cancel();
	composed_op_dispatch_result(self, error, 0);
	timed_handshake_release(self);
}

} /* namespace */

void timed_handshake_op::destroy(composed_op *base)
{
	timed_handshake_op *self = static_cast<timed_handshake_op *>(base);
	self->~timed_handshake_op();
	free(self);
}

void timed_handshake_op::complete(composed_op *base)
{
	timed_handshake_op *self = static_cast<timed_handshake_op *>(base);
		self->callback_(self->context_, self->result_error_);
}

void timed_handshake_op::cancel(composed_op *base, cancellation_type type)
{
	timed_handshake_op *self = static_cast<timed_handshake_op *>(base);
	if (type == cancellation_type::none)
		return;
	self->io_cancel_.emit(type);
	self->timer_->cancel();
}

void timed_handshake_op::handshake_cb(void *ctx, async_error_code error)
{
	timed_handshake_op *self = static_cast<timed_handshake_op *>(ctx);
	timed_handshake_finish(self, error);
}

void timed_handshake_op::timer_cb(void *ctx, async_error_code error)
{
	timed_handshake_op *self = static_cast<timed_handshake_op *>(ctx);
	if (error)
	{
		timed_handshake_release(self);
		return;
	}
	if (!composed_op_try_complete(self))
	{
		timed_handshake_release(self);
		return;
	}

	self->io_cancel_.emit(cancellation_type::terminal);
	composed_op_dispatch_result(self, async_socket_error(WSAETIMEDOUT), 0);
	timed_handshake_release(self);
}

int timed_handshake_start(ssl_stream *stream, steady_timer *timer,
						  int timeout_ms,
						  void (*callback)(void *, async_error_code), void *context,
						  cancellation_slot slot, void (*destroy_context)(void *))
{
	if (!stream || !timer || !callback)
	{
		errno = EINVAL;
		return -1;
	}

	void *mem = malloc(sizeof(timed_handshake_op));
	if (!mem)
	{
		errno = ENOMEM;
		return -1;
	}

	timed_handshake_op *op = new (mem) timed_handshake_op();
	op->stream_ = stream;
	op->timer_ = timer;
	op->callback_ = callback;
	op->context_ = context;
	composed_op_set_ctx(op, destroy_context, context);
	composed_op_set_executor(op, stream->get_executor());
	composed_op_set_cancellation(op, slot, &timed_handshake_op::cancel);

	timer->expires_after(std::chrono::milliseconds(timeout_ms));
	composed_op_add_ref(op);
	if (timer->async_wait(&timed_handshake_op::timer_cb, op,
						 composed_op_destroy) != 0)
	{
		int error = errno;
		composed_op_release(op);
		composed_op_release(op);
		errno = error;
		return -1;
	}

	composed_op_add_ref(op);
	if (stream->async_handshake(&timed_handshake_op::handshake_cb, op,
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

