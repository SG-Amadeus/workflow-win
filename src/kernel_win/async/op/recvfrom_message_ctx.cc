#include "recvfrom_message_ctx.h"
#include "../steady_timer.h"
#include "async_handler.h"

#include <errno.h>
#include <limits.h>
#include <new>
#include <stdlib.h>
#include <string.h>
#include <windows.h>

namespace
{

static void recvfrom_message_cb(void *, async_error_code, size_t);

static void recvfrom_message_cancel(void *arg, cancellation_type type)
{
	recvfrom_message_ctx *ctx = static_cast<recvfrom_message_ctx *>(arg);
	ctx->read_cancel.emit(type);
}

static steady_timer *recv_timer_create(executor ex)
{
	return steady_timer::create(ex);
}

static void recv_timer_destroy(steady_timer *t)
{
	steady_timer::destroy(t);
}

static recvfrom_message_ctx *recvfrom_message_ctx_create(
	udp_socket *socket, void *buf, size_t sz,
	read_message_filter f, void *fu,
	void (*cb)(void *, async_error_code, size_t), void *ctx,
	struct sockaddr *a, int *al, int timeout, void (*ab)(void *),
	const cancellation_slot &slot, executor ex)
{
	void *mem = malloc(sizeof(recvfrom_message_ctx));
	void *owned_buffer = nullptr;
	if (!buf)
	{
		owned_buffer = malloc(sz);
		if (!owned_buffer)
		{
			free(mem);
			return nullptr;
		}
	}
	recvfrom_message_ctx *self = static_cast<recvfrom_message_ctx *>(mem);
	if (!self)
	{
		free(owned_buffer);
		return nullptr;
	}
	new (self) recvfrom_message_ctx();

	self->socket = socket;
	self->buffer = buf ? buf : owned_buffer;
	self->owned_buffer = owned_buffer;
	self->size = sz;
	self->filter = f;
	self->filter_userdata = fu;
	self->callback = cb;
	self->destroy = ab;
	self->context = ctx;
	self->executor_ = ex;
	if (a)
	{
		self->addr = a;
		self->addrlen = al;
	}
	else
	{
		self->owned_addrlen = sizeof self->owned_addr;
		self->addr = reinterpret_cast<struct sockaddr *>(&self->owned_addr);
		self->addrlen = &self->owned_addrlen;
	}
	self->timer = nullptr;
	self->timeout_ms = timeout;
	self->refs = 1;
	self->done = 0;
	self->cancel_state.connect(slot);
	self->cancel_state.set_notify(&recvfrom_message_cancel, self);
	::InitializeCriticalSection(&self->mutex_);
	return self;
}

static void recvfrom_message_ctx_release(recvfrom_message_ctx *self)
{
	if (::InterlockedDecrement(&self->refs) == 0)
	{
		if (self->timer)
			recv_timer_destroy(self->timer);
		free(self->owned_buffer);
		::DeleteCriticalSection(&self->mutex_);
		self->~recvfrom_message_ctx();
		free(self);
	}
}

static void recvfrom_message_ctx_destroy(void *arg)
{
	recvfrom_message_ctx *ctx = static_cast<recvfrom_message_ctx *>(arg);
	steady_timer *timer = nullptr;
	bool first;
	::EnterCriticalSection(&ctx->mutex_);
	first = !ctx->done;
	ctx->done = 1;
	if (first)
	{
		timer = ctx->timer;
		ctx->timer = nullptr;
	}
	::LeaveCriticalSection(&ctx->mutex_);
	if (timer)
	{
		timer->cancel();
		recv_timer_destroy(timer);
	}
	if (first && ::InterlockedCompareExchange(&ctx->destroy_called, 1, 0) == 0 &&
		ctx->destroy)
		ctx->destroy(ctx->context);
	recvfrom_message_ctx_release(ctx);
}

static void recvfrom_message_complete(void *arg, async_error_code /*error*/,
									 size_t /*bytes*/)
{
	recvfrom_message_ctx *ctx = static_cast<recvfrom_message_ctx *>(arg);
	ctx->callback(ctx->context, ctx->result_error, ctx->result_bytes);
	recvfrom_message_ctx_release(ctx);
}

static void recvfrom_message_complete_destroy(void *arg)
{
	recvfrom_message_ctx *ctx = static_cast<recvfrom_message_ctx *>(arg);
	if (::InterlockedCompareExchange(&ctx->destroy_called, 1, 0) == 0 &&
		ctx->destroy)
		ctx->destroy(ctx->context);
	recvfrom_message_ctx_release(ctx);
}

static void recvfrom_message_deliver(recvfrom_message_ctx *ctx,
								 async_error_code error, size_t bytes)
{
	ctx->result_error = error;
	ctx->result_bytes = bytes;
	::InterlockedIncrement(&ctx->refs);
	async_handler handler = {
		&recvfrom_message_complete, ctx, &recvfrom_message_complete_destroy
	};
	/* On dispatch failure the handler was destroyed inside
	 * async_handler_dispatch (it released the reference). */
	async_handler_dispatch(ctx->executor_, handler, async_error_code(), 0);
}

static bool recvfrom_message_ctx_finish_once(recvfrom_message_ctx *self,
																 async_error_code error, size_t bytes,
																 bool cancel_read = false)
{
	steady_timer *t = nullptr;
	bool first;

	::EnterCriticalSection(&self->mutex_);
	first = !self->done;
	self->done = 1;
	if (first)
	{
		t = self->timer;
		self->timer = nullptr;
	}
	::LeaveCriticalSection(&self->mutex_);

	if (t)
	{
		t->cancel();
		recv_timer_destroy(t);
	}

	if (first)
	{
		/* The socket must be cancelled while the composed context still owns
		 * the operation.  Delivering first may synchronously release the
		 * connection that contains this UDP socket. */
		if (cancel_read)
			self->read_cancel.emit(cancellation_type::terminal);
		recvfrom_message_deliver(self, error, bytes);
	}
	return first;
}

/* Returns 0 when the next receive has been submitted, 1 when the
 * operation has already finished, or -errno when submission failed. */
static int recvfrom_message_ctx_continue_recv(recvfrom_message_ctx *self)
{
	::EnterCriticalSection(&self->mutex_);
	if (self->done)
	{
		::LeaveCriticalSection(&self->mutex_);
		return 1;
	}
	int rc = self->socket->async_receive_from(
		self->buffer, self->size, self->addr, self->addrlen,
		recvfrom_message_cb, self, self->read_cancel.slot(),
		recvfrom_message_ctx_destroy);
	::LeaveCriticalSection(&self->mutex_);
	if (rc == 0)
		return 0;
	return -errno;
}

static void recvfrom_message_timer_cb(void *arg, async_error_code error)
{
	recvfrom_message_ctx *ctx = static_cast<recvfrom_message_ctx *>(arg);
	if (!error)
		recvfrom_message_ctx_finish_once(ctx,
			async_socket_error(WSAETIMEDOUT), 0, true);

	recvfrom_message_ctx_release(ctx);
}

static void recvfrom_message_cb(void *arg, async_error_code error, size_t bytes)
{
	recvfrom_message_ctx *ctx = static_cast<recvfrom_message_ctx *>(arg);

	if (error)
	{
		recvfrom_message_ctx_finish_once(ctx, error, 0);
		recvfrom_message_ctx_release(ctx);
		return;
	}

	size_t consumed = bytes;
	int ret = ctx->filter(ctx->buffer, &consumed, ctx->filter_userdata);
	if (ret > 0)
	{
		recvfrom_message_ctx_finish_once(ctx, async_error_code(), consumed);
		recvfrom_message_ctx_release(ctx);
		return;
	}

	if (ret < 0)
	{
		recvfrom_message_ctx_finish_once(ctx,
			async_error_from_errno(errno ? errno : EIO), consumed);
		recvfrom_message_ctx_release(ctx);
		return;
	}

	/* Filter asked for another datagram: receive again. */
	int rc = recvfrom_message_ctx_continue_recv(ctx);
	if (rc > 0)
	{
		recvfrom_message_ctx_release(ctx);
		return;
	}
	if (rc < 0)
	{
		recvfrom_message_ctx_finish_once(ctx, async_error_from_errno(-rc), 0);
		recvfrom_message_ctx_release(ctx);
	}
}

} /* namespace */

int async_start_recvfrom_message(udp_socket *socket,
								  void *buffer, size_t size,
								  struct sockaddr *addr, int *addrlen,
								  read_message_filter filter,
								  void *filter_userdata,
								  void (*callback)(void *, async_error_code, size_t),
								  void *context, int timeout_ms,
								  const cancellation_slot &slot,
								  void (*destroy)(void *), bool exact_timeout)
{
	if (!socket || size == 0 || (addr && !addrlen) || (!addr && addrlen) ||
		!filter || !callback)
	{
		errno = EINVAL;
		return -1;
	}

	executor ex = socket ? socket->get_executor() : executor();
	if (!ex.context())
	{
		errno = EINVAL;
		return -1;
	}

	recvfrom_message_ctx *ctx = recvfrom_message_ctx_create(
		socket, buffer, size, filter, filter_userdata, callback, context,
		addr, addrlen, timeout_ms, destroy, slot, ex);
	if (!ctx)
	{
		errno = ENOMEM;
		return -1;
	}

	if (timeout_ms > 0 || (exact_timeout && timeout_ms == 0))
	{
		steady_timer *timer = recv_timer_create(ex);
		if (!timer)
		{
			recvfrom_message_ctx_release(ctx);
			errno = ENOMEM;
			return -1;
		}
		ctx->timer = timer;
		timer->expires_after(std::chrono::milliseconds(timeout_ms));

		::InterlockedIncrement(&ctx->refs);
		if (timer->async_wait(recvfrom_message_timer_cb, ctx,
							  recvfrom_message_ctx_destroy) != 0)
		{
			::InterlockedDecrement(&ctx->refs);
			recv_timer_destroy(ctx->timer);
			ctx->timer = nullptr;
			recvfrom_message_ctx_release(ctx);
			errno = EIO;
			return -1;
		}
	}

	/* The base ref keeps ctx alive while the receive is being submitted.
	 * The timer may already have fired on another worker; the lock prevents
	 * a receive from being started after the timeout has completed. */
	::InterlockedIncrement(&ctx->refs);
	int rc;
	::EnterCriticalSection(&ctx->mutex_);
	if (ctx->done)
		rc = 1;
	else if (socket->async_receive_from(ctx->buffer, ctx->size,
									   ctx->addr, ctx->addrlen,
									   recvfrom_message_cb, ctx,
									   ctx->read_cancel.slot(),
									   recvfrom_message_ctx_destroy) == 0)
		rc = 0;
	else
		rc = -errno;
	::LeaveCriticalSection(&ctx->mutex_);

	if (rc == 1)
	{
		::InterlockedDecrement(&ctx->refs);
		recvfrom_message_ctx_release(ctx);
		return 0;
	}

	if (rc < 0)
	{
		async_error_code error = async_error_from_errno(-rc);
		::InterlockedDecrement(&ctx->refs);

		/* A submitted timer makes this a composed operation, not a synchronous
		 * initiation failure.  Finish it through the normal exactly-once path;
		 * the timer's destroy callback then owns the remaining child reference. */
		::EnterCriticalSection(&ctx->mutex_);
		bool timer_submitted = ctx->timer != nullptr;
		::LeaveCriticalSection(&ctx->mutex_);
		if (timer_submitted)
		{
			recvfrom_message_ctx_finish_once(ctx, error, 0);
			recvfrom_message_ctx_release(ctx);
			return 0;
		}

		recvfrom_message_ctx_release(ctx);
		errno = async_error_to_errno(error);
		return -1;
	}

	recvfrom_message_ctx_release(ctx);
	return 0;
}
