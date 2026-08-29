/*
  AsyncCore: steady_timer operation implementation.
*/

#include "timer_wait_op.h"
#include "async_handler.h"
#include "op_pools.h"

#include <cstdlib>
#include <new>

steady_timer_wait_op *timer_wait_op_alloc(op_pools *pools)
{
	steady_timer_wait_op *op = op_pools_alloc_timer_wait(pools);
	if (op)
	{
		new (op) steady_timer_wait_op();
		op->pools_ = pools;
	}
	return op;
}

void timer_wait_op_free(steady_timer_wait_op *op)
{
	if (!op)
		return;
	if (op->impl_)
	{
		steady_timer_impl_release(op->impl_);
		op->impl_ = nullptr;
	}
	op_pools_free_timer_wait(op->pools_, op);
}

namespace
{

struct timer_call
{
	void (*callback)(void *, async_error_code);
	void (*destroy)(void *);
	void *context;
};

static void timer_call_handler(void *arg, async_error_code error, size_t)
{
	timer_call *call = static_cast<timer_call *>(arg);
	call->callback(call->context, error);
	free(call);
}

static void timer_call_destroy(void *arg)
{
	timer_call *call = static_cast<timer_call *>(arg);
	if (call->destroy)
		call->destroy(call->context);
	free(call);
}

} /* namespace */

void steady_timer_wait_op::do_complete(void *owner, win_iocp_operation *base,
									   async_error_code /*error*/, size_t /*bytes*/)
{
	steady_timer_wait_op *self =
		static_cast<steady_timer_wait_op *>(base);
	void (*callback)(void *, async_error_code) = self->callback_;
	void (*destroy)(void *) = self->destroy_;
	void *context = self->context_;
	async_error_code error = self->error;
	handler_work completion_work(static_cast<handler_work &&>(self->work_));
	timer_wait_op_free(self);

	if (!owner)
	{
		if (destroy)
			destroy(context);
		return;
	}

	timer_call *call = static_cast<timer_call *>(malloc(sizeof *call));
	if (!call)
	{
		if (destroy)
			destroy(context);
		return;
	}
	call->callback = callback;
	call->destroy = destroy;
	call->context = context;
	async_handler handler = {
		&timer_call_handler, call, &timer_call_destroy
	};
	(void)async_handler_dispatch(completion_work, handler, error, 0);
}
