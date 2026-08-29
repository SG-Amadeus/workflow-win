#include "timed_ssl_write_op.h"

#include <errno.h>
#include <new>
#include <stdlib.h>

namespace
{

void timed_ssl_write_release(timed_ssl_write_op *self)
{
	composed_op_release(self);
}

void timed_ssl_write_finish(timed_ssl_write_op *self, async_error_code error,
							size_t bytes)
{
	if (!composed_op_try_complete(self))
		return;

	self->timer_->cancel();
	composed_op_dispatch_result(self, error, bytes);
}

} /* namespace */

void timed_ssl_write_op::destroy(composed_op *base)
{
	timed_ssl_write_op *self = static_cast<timed_ssl_write_op *>(base);
	self->~timed_ssl_write_op();
	free(self);
}

void timed_ssl_write_op::complete(composed_op *base)
{
	timed_ssl_write_op *self = static_cast<timed_ssl_write_op *>(base);
		self->callback_(self->context_, self->result_error_, self->result_bytes_);
}

void timed_ssl_write_op::cancel(composed_op *base,
							cancellation_type type)
{
	timed_ssl_write_op *self = static_cast<timed_ssl_write_op *>(base);
	if (type == cancellation_type::none)
		return;
	self->io_cancel_.emit(type);
	self->timer_->cancel();
}

void timed_ssl_write_op::write_cb(void *ctx, async_error_code error, size_t bytes)
{
	timed_ssl_write_op *self = static_cast<timed_ssl_write_op *>(ctx);
	/* async_writev is the sequential composed child. */
	timed_ssl_write_finish(self, error, bytes);
}

void timed_ssl_write_op::timer_cb(void *ctx, async_error_code error)
{
	timed_ssl_write_op *self = static_cast<timed_ssl_write_op *>(ctx);
	if (error)
	{
		timed_ssl_write_release(self);
		return;
	}

	if (composed_op_try_complete(self))
	{
		self->io_cancel_.emit(cancellation_type::terminal);
		composed_op_dispatch_result(self, async_socket_error(WSAETIMEDOUT), 0);
	}
	timed_ssl_write_release(self);
}

int timed_ssl_write_start(ssl_stream *stream, steady_timer *timer,
						  const struct iovec *iov, int iovcnt,
						  int timeout_ms,
						  void (*callback)(void *, async_error_code, size_t),
						  void *context, cancellation_slot slot,
						  void (*destroy_context)(void *))
{
	if (!stream || !timer || !iov || iovcnt <= 0 || !callback)
	{
		errno = EINVAL;
		return -1;
	}

	void *mem = malloc(sizeof(timed_ssl_write_op));
	if (!mem)
	{
		errno = ENOMEM;
		return -1;
	}

	timed_ssl_write_op *op = new (mem) timed_ssl_write_op();
	op->stream_ = stream;
	op->timer_ = timer;
	op->callback_ = callback;
	op->context_ = context;
	composed_op_set_ctx(op, destroy_context, context);
	composed_op_set_executor(op, stream->get_executor());
	composed_op_set_cancellation(op, slot, &timed_ssl_write_op::cancel);

	timer->expires_after(std::chrono::milliseconds(timeout_ms));
	composed_op_add_ref(op);
	if (timer->async_wait(&timed_ssl_write_op::timer_cb, op,
						 composed_op_destroy) != 0)
	{
		int error = errno;
		composed_op_release(op);
		composed_op_release(op);
		errno = error;
		return -1;
	}

	composed_op_add_ref(op);
	if (async_write(stream, iov, iovcnt, &timed_ssl_write_op::write_cb,
					 op, op->io_cancel_.slot(), composed_op_release_context) != 0)
	{
		async_error_code error = async_error_from_errno(errno ? errno : EIO);
		composed_op_release(op);
		composed_op_complete_with_error(op, error, 0);
		composed_op_release(op);
		return 0;
	}

	composed_op_release(op);
	return 0;
}

