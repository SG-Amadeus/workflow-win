#include "timed_udp_send_op.h"

#include <errno.h>
#include <new>
#include <stdlib.h>

namespace
{

void timed_udp_send_release(timed_udp_send_op *self)
{
	composed_op_release(self);
}

void timed_udp_send_finish(timed_udp_send_op *self, async_error_code error, size_t bytes)
{
	if (!composed_op_try_complete(self))
	{
		timed_udp_send_release(self);
		return;
	}

	if (self->timer_)
		self->timer_->cancel();
	composed_op_dispatch_result(self, error, bytes);
	timed_udp_send_release(self);
}

} /* namespace */

void timed_udp_send_op::destroy(composed_op *base)
{
	timed_udp_send_op *self = static_cast<timed_udp_send_op *>(base);
	self->~timed_udp_send_op();
	free(self);
}

void timed_udp_send_op::complete(composed_op *base)
{
	timed_udp_send_op *self = static_cast<timed_udp_send_op *>(base);
		self->callback_(self->context_, self->result_error_, self->result_bytes_);
}

void timed_udp_send_op::cancel(composed_op *base, cancellation_type type)
{
	timed_udp_send_op *self = static_cast<timed_udp_send_op *>(base);
	if (type == cancellation_type::none)
		return;
	self->io_cancel_.emit(type);
	self->timer_->cancel();
}

void timed_udp_send_op::send_cb(void *ctx, async_error_code error, size_t bytes)
{
	timed_udp_send_op *self = static_cast<timed_udp_send_op *>(ctx);
	timed_udp_send_finish(self, error, bytes);
}

void timed_udp_send_op::timer_cb(void *ctx, async_error_code error)
{
	timed_udp_send_op *self = static_cast<timed_udp_send_op *>(ctx);
	if (error)
	{
		timed_udp_send_release(self);
		return;
	}
	if (!composed_op_try_complete(self))
	{
		timed_udp_send_release(self);
		return;
	}

	self->io_cancel_.emit(cancellation_type::terminal);
	composed_op_dispatch_result(self, async_socket_error(WSAETIMEDOUT), 0);
	timed_udp_send_release(self);
}

int timed_udp_send_start(udp_socket *socket, steady_timer *timer,
						 const struct iovec *iov, int iovcnt,
						 const struct sockaddr *addr, int addrlen,
						 int timeout_ms,
						 void (*callback)(void *, async_error_code, size_t),
						 void *context, cancellation_slot slot,
						 void (*destroy_context)(void *))
{
	if (!socket || !timer || !iov || iovcnt <= 0 || !addr || addrlen <= 0 ||
		!callback)
	{
		errno = EINVAL;
		return -1;
	}

	void *mem = malloc(sizeof(timed_udp_send_op));
	if (!mem)
	{
		errno = ENOMEM;
		return -1;
	}

	timed_udp_send_op *op = new (mem) timed_udp_send_op();
	op->socket_ = socket;
	op->timer_ = timer;
	op->callback_ = callback;
	op->context_ = context;
	composed_op_set_ctx(op, destroy_context, context);
	composed_op_set_executor(op, socket->get_executor());
	composed_op_set_cancellation(op, slot, &timed_udp_send_op::cancel);
	op->iov_ = iov;
	op->iovcnt_ = iovcnt;
	op->addr_ = addr;
	op->addrlen_ = addrlen;

	timer->expires_after(std::chrono::milliseconds(timeout_ms));
	composed_op_add_ref(op);
	if (timer->async_wait(&timed_udp_send_op::timer_cb, op,
						 composed_op_destroy) != 0)
	{
		int error = errno;
		composed_op_release(op);
		composed_op_release(op);
		errno = error;
		return -1;
	}

	composed_op_add_ref(op);
	if (socket->async_sendto_v(iov, iovcnt, addr, addrlen,
								&timed_udp_send_op::send_cb, op,
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

