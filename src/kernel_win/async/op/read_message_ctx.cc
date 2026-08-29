#include "read_message_ctx.h"
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
static void read_message_cb(void *, async_error_code, size_t);
static void read_message_destroy(void *);
static void ctx_acquire(read_message_ctx *ctx);
static void ctx_release(read_message_ctx *ctx);
static bool finish_once(read_message_ctx *ctx, async_error_code error, size_t bytes);

static void read_message_cancel(void *arg, cancellation_type type)
{
	read_message_ctx *ctx = static_cast<read_message_ctx *>(arg);
	ctx->read_cancel.emit(type);
}

static void read_message_destroy_once(read_message_ctx *ctx)
{
	if (::InterlockedCompareExchange(&ctx->destroy_called, 1, 0) == 0 &&
		ctx->destroy)
		ctx->destroy(ctx->context);
}

static void read_message_complete(void *arg, async_error_code /*error*/,
								  size_t /*bytes*/)
{
	read_message_ctx *ctx = static_cast<read_message_ctx *>(arg);
	ctx->callback(ctx->context, ctx->result_error, ctx->result_bytes);
	ctx_release(ctx);
}

static void read_message_complete_destroy(void *arg)
{
	read_message_ctx *ctx = static_cast<read_message_ctx *>(arg);
	read_message_destroy_once(ctx);
	ctx_release(ctx);
}

static void read_message_deliver(read_message_ctx *ctx, async_error_code error, size_t bytes)
{
	ctx->result_error = error;
	ctx->result_bytes = bytes;
	ctx_acquire(ctx);
	async_handler handler = {
		&read_message_complete, ctx, &read_message_complete_destroy
	};
	/* On dispatch failure the handler was destroyed inside
	 * async_handler_dispatch (it released the reference). */
	async_handler_dispatch(ctx->executor_, handler, async_error_code(), 0);
}

static steady_timer *timer_create(executor ex)
{
	return steady_timer::create(ex);
}

static void timer_destroy(steady_timer *timer)
{
	steady_timer::destroy(timer);
}

static void ctx_acquire(read_message_ctx *ctx)
{
	::InterlockedIncrement(&ctx->refs);
}

static void ctx_release(read_message_ctx *ctx)
{
	if (::InterlockedDecrement(&ctx->refs) == 0)
	{
		timer_destroy(ctx->timer);
		free(ctx->owned_buffer);
		::DeleteCriticalSection(&ctx->lock);
		ctx->~read_message_ctx();
		free(ctx);
	}
}

static int timeout_min(int first, int second)
{
	if (first < 0)
		return second;
	if (second < 0)
		return first;
	return first < second ? first : second;
}

static int total_time_left(read_message_ctx *ctx)
{
	if (ctx->total_timeout < 0)
		return -1;
	ULONGLONG now = ::GetTickCount64();
	if (now >= ctx->total_deadline)
		return 0;
	ULONGLONG left = ctx->total_deadline - now;
	return left > INT_MAX ? INT_MAX : (int)left;
}

static int current_timeout(read_message_ctx *ctx)
{
	if (ctx->renew_flag && ::InterlockedExchange(ctx->renew_flag, 0))
	{
		ctx->first_phase = false;
		ctx->total_timeout = -1;
	}
	if (ctx->first_phase && ctx->first_timeout != 0)
		return ctx->first_timeout;
	return timeout_min(ctx->next_timeout, total_time_left(ctx));
}

static void timer_wait_release(read_timer_wait *wait)
{
	read_message_ctx *ctx = wait->ctx;
	free(wait);
	ctx_release(ctx);
}

static void timer_wait_destroy(void *arg)
{
	timer_wait_release(static_cast<read_timer_wait *>(arg));
}

static void timer_wait_cb(void *arg, async_error_code error)
{
	read_timer_wait *wait = static_cast<read_timer_wait *>(arg);
	read_message_ctx *ctx = wait->ctx;
	bool timeout = false;

	if (!error)
	{
		::EnterCriticalSection(&ctx->lock);
		if (!ctx->done && wait->generation == ctx->timer_generation)
			timeout = true;
		::LeaveCriticalSection(&ctx->lock);
	}

	if (timeout)
	{
		/* Cancel the child before delivering the composed completion.  The
		 * completion may run inline on the strand and release the owning
		 * connection, so the socket must not be touched afterwards. */
		ctx->read_cancel.emit(cancellation_type::terminal);
		finish_once(ctx, async_socket_error(WSAETIMEDOUT), 0);
	}
	timer_wait_release(wait);
}

static int arm_timer(read_message_ctx *ctx)
{
	int timeout = current_timeout(ctx);
	read_timer_wait *wait = nullptr;
	if (timeout >= 0)
	{
		wait = (read_timer_wait *)malloc(sizeof *wait);
		if (!wait)
		{
			errno = ENOMEM;
			return -1;
		}
	}

	::EnterCriticalSection(&ctx->lock);
	LONG generation = ++ctx->timer_generation;
	if (ctx->done)
	{
		::LeaveCriticalSection(&ctx->lock);
		free(wait);
		return 1;
	}
	if (timeout < 0)
	{
		ctx->timer->cancel();
		::LeaveCriticalSection(&ctx->lock);
		return 0;
	}

	wait->ctx = ctx;
	wait->generation = generation;
	ctx->timer->expires_after(std::chrono::milliseconds(timeout));
	ctx_acquire(ctx);
	int ret = ctx->timer->async_wait(timer_wait_cb, wait,
									 timer_wait_destroy);
	::LeaveCriticalSection(&ctx->lock);
	if (ret != 0)
	{
		timer_wait_release(wait);
		return -1;
	}
	return 0;
}

static bool finish_once(read_message_ctx *ctx, async_error_code error, size_t bytes)
{
	bool first;
	::EnterCriticalSection(&ctx->lock);
	first = !ctx->done;
	if (first)
	{
		ctx->done = 1;
		++ctx->timer_generation;
		ctx->timer->cancel();
	}
	::LeaveCriticalSection(&ctx->lock);
	if (first)
		read_message_deliver(ctx, error, bytes);
	return first;
}

static int continue_read(read_message_ctx *ctx, void *buf, size_t size)
{
	if (size == 0)
	{
		errno = EMSGSIZE;
		return -1;
	}
	int ret = arm_timer(ctx);
	if (ret != 0)
		return ret;
	return ctx->submit(ctx->socket, buf, size, read_message_cb, ctx,
						 ctx->read_cancel.slot(),
						 read_message_destroy);
}

static void read_message_cb(void *arg, async_error_code error, size_t bytes)
{
	read_message_ctx *ctx = static_cast<read_message_ctx *>(arg);
	if (::InterlockedCompareExchange(&ctx->done, 0, 0))
	{
		ctx_release(ctx);
		return;
	}
	if (error || bytes == 0)
	{
		finish_once(ctx, error ? error :
			async_generic_error(async_error_eof), 0);
		ctx_release(ctx);
		return;
	}

	if (ctx->first_phase)
	{
		ctx->first_phase = false;
		/* A custom first timeout owns the first phase.  Otherwise the
		 * receive timeout is consumed when it wins the first read; only a
		 * longer receive deadline continues across subsequent reads. */
		if (ctx->first_timeout != 0 ||
			(ctx->total_timeout >= 0 &&
			 (ctx->next_timeout < 0 ||
			  ctx->total_timeout <= ctx->next_timeout)))
			ctx->total_timeout = -1;
	}

	size_t total = ctx->pending + bytes;
	size_t consumed = total;
	int ret = ctx->filter(ctx->buffer, &consumed, ctx->filter_userdata);
	if (consumed > total || (ret > 0 && consumed < total))
	{
		finish_once(ctx, async_error_from_errno(EBADMSG), total);
		ctx_release(ctx);
		return;
	}
	if (ret > 0)
	{
		ctx->pending = 0;
		finish_once(ctx, async_error_code(), consumed);
		ctx_release(ctx);
		return;
	}
	if (ret < 0)
	{
		finish_once(ctx, async_error_from_errno(errno ? errno : EIO), consumed);
		ctx_release(ctx);
		return;
	}

	ctx->pending = total - consumed;
	if (ctx->pending)
		memmove(ctx->buffer, (char *)ctx->buffer + consumed, ctx->pending);
	char *next = (char *)ctx->buffer + ctx->pending;
	size_t available = ctx->size - ctx->pending;
	ret = continue_read(ctx, next, available);
	if (ret != 0)
	{
		if (ret < 0)
			finish_once(ctx, async_error_from_errno(errno ? errno : EIO), 0);
		ctx_release(ctx);
	}
}

static void read_message_destroy(void *arg)
{
	read_message_ctx *ctx = static_cast<read_message_ctx *>(arg);
	::EnterCriticalSection(&ctx->lock);
	if (!ctx->done)
	{
		ctx->done = 1;
		++ctx->timer_generation;
		ctx->timer->cancel();
	}
	::LeaveCriticalSection(&ctx->lock);
	read_message_destroy_once(ctx);
	ctx_release(ctx);
}
} /* namespace */

int async_start_read_message(void *socket, read_submit_t submit,
								  read_executor_t get_executor,
								  void *buffer, size_t size,
								  read_message_filter filter,
								  void *filter_userdata,
								  void (*callback)(void *, async_error_code, size_t),
								  void *context, int first_timeout,
								  int next_timeout, int total_timeout,
								  const cancellation_slot &slot,
								  void (*destroy)(void *),
								  volatile LONG *renew_flag)
{
	if (!socket || size == 0 || !filter || !callback)
	{
		errno = EINVAL;
		return -1;
	}
	executor ex = get_executor(socket);
	if (!ex.context())
	{
		errno = EINVAL;
		return -1;
	}

	void *owned_buffer = nullptr;
	if (!buffer)
	{
		owned_buffer = malloc(size);
		if (!owned_buffer)
		{
			errno = ENOMEM;
			return -1;
		}
	}

	void *mem = malloc(sizeof(read_message_ctx));
	read_message_ctx *ctx = static_cast<read_message_ctx *>(mem);
	if (!ctx)
	{
		free(owned_buffer);
		errno = ENOMEM;
		return -1;
	}
	new (ctx) read_message_ctx();
	ctx->socket = socket;
	ctx->submit = submit;
	ctx->cancel_state.connect(slot);
	ctx->cancel_state.set_notify(&read_message_cancel, ctx);
	ctx->buffer = buffer ? buffer : owned_buffer;
	ctx->owned_buffer = owned_buffer;
	ctx->size = size;
	ctx->filter = filter;
	ctx->filter_userdata = filter_userdata;
	ctx->callback = callback;
	ctx->destroy = destroy;
	ctx->context = context;
	ctx->first_timeout = first_timeout;
	ctx->next_timeout = next_timeout;
	ctx->total_timeout = total_timeout;
	ctx->renew_flag = renew_flag;
	ctx->executor_ = ex;
	ctx->total_deadline = total_timeout >= 0 ?
		::GetTickCount64() + (ULONGLONG)total_timeout : 0;
	ctx->first_phase = true;
	ctx->refs = 1;
	::InitializeCriticalSection(&ctx->lock);
	ctx->timer = timer_create(ex);
	if (!ctx->timer)
	{
		ctx_release(ctx);
		errno = ENOMEM;
		return -1;
	}

	int ret = arm_timer(ctx);
	if (ret != 0)
	{
		ctx_release(ctx);
		return ret < 0 ? -1 : 0;
	}
	ctx_acquire(ctx);
	ret = submit(socket, ctx->buffer, size, read_message_cb, ctx,
					 ctx->read_cancel.slot(),
					read_message_destroy);
	if (ret != 0)
	{
		async_error_code error = async_error_from_errno(errno ? errno : EIO);
		/* The timer is already an outstanding child operation.  Complete the
		 * composed operation asynchronously instead of returning a partial
		 * start failure while its destroy callback still owns ctx. */
		finish_once(ctx, error, 0);
		ctx_release(ctx);
		return 0;
	}
	ctx_release(ctx);
	return 0;
}



