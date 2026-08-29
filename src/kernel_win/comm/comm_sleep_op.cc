#include "comm_sleep_op.h"

#include <assert.h>
#include <errno.h>
#include <stdlib.h>

#include <climits>
#include <new>

void comm_sleep_op::retire_io(comm_sleep_op *self, async_error_code error,
							  bool destroyed)
{
	SleepSession *session = self->session_;
	if (!session)
		return;
	int error_value = async_error_to_errno(error);

	/* Freeze the logical result before retiring the timer ownership. */
	self->session_ = session;
	AcquireSRWLockExclusive(&self->impl_->sleep_lock);
	if (!list_empty(&self->live_list_))
	{
		list_del(&self->live_list_);
		INIT_LIST_HEAD(&self->live_list_);
	}
	ReleaseSRWLockExclusive(&self->impl_->sleep_lock);
	if (destroyed || ::InterlockedCompareExchange(&session->state, 0, 0) == 2)
	{
		self->result_state_ = SS_STATE_DISRUPTED;
		self->result_error_ = ECANCELED;
	}
	else if (error_value == ECANCELED)
	{
		self->result_state_ = SS_STATE_ERROR;
		self->result_error_ = ECANCELED;
	}
	else if (error_value == 0)
	{
		self->result_state_ = SS_STATE_COMPLETE;
		self->result_error_ = 0;
	}
	else
	{
		self->result_state_ = SS_STATE_ERROR;
		self->result_error_ = async_error_to_errno(error);
	}

	session->timer_handle = nullptr;
	::InterlockedExchange(&session->state, 0);
}

comm_sleep_op::comm_sleep_op()
	: timer_(), impl_(nullptr), session_(nullptr), result_state_(0),
	  result_error_(0), handler_completed_(0)
{
	INIT_LIST_HEAD(&live_list_);
	composed_op_init(this, &comm_sleep_op::destroy,
					 &comm_sleep_op::complete);
}

void comm_sleep_op::destroy(composed_op *base)
{
	comm_sleep_op *self = static_cast<comm_sleep_op *>(base);
	if (::InterlockedCompareExchange(&self->handler_completed_, 0, 0) == 0)
	{
		/* A context teardown may destroy the timer operation without dispatching
		 * its handler.  Keep the operation alive until the handler pool consumes
		 * the disruption result. */
		comm_sleep_op::post_completion(self,
			async_error_from_errno(ECANCELED), true);
		return;
	}
	self->~comm_sleep_op();
	free(self);
}

void comm_sleep_op::complete(composed_op *base)
{
	comm_sleep_op *self = static_cast<comm_sleep_op *>(base);
	SleepSession *session = self->session_;
	self->session_ = nullptr;
	if (session)
		session->handle(self->result_state_, self->result_error_);
}

void comm_sleep_op::timer_cb(void *ctx, async_error_code error)
{
	comm_sleep_op *self = static_cast<comm_sleep_op *>(ctx);
	comm_sleep_op::post_completion(self, error, false);

	composed_op_release(self);
}

void comm_sleep_op::handle_complete(void *ctx)
{
	comm_sleep_op *self = static_cast<comm_sleep_op *>(ctx);
	::InterlockedExchange(&self->handler_completed_, 1);
	composed_op_complete_handler(self);
	composed_op_release(self);
}

void comm_sleep_op::post_completion(comm_sleep_op *self,
								async_error_code error,
								bool destroyed)
{
	if (!composed_op_try_complete(self))
		return;
	comm_sleep_op::retire_io(self, error, destroyed);
	self->result_bytes_ = 0;
	/* Timer completion has left the ASIO execution domain. */
	self->work_.reset();
	::InterlockedExchange(&self->result_ready_, 1);
	::InterlockedExchange(&self->dispatch_started_, 1);
	composed_op_add_ref(self);
	int ret = self->impl_->post_handler(&comm_sleep_op::handle_complete,
			self, &self->handler_task_);
	/* Accepted sleeps are drained before the handler pool is destroyed. */
	assert(ret == 0);
	if (ret != 0)
		RaiseFailFastException(nullptr, nullptr, 0);
}

int comm_sleep_op::cancel(SleepSession *session)
{
	if (!session || ::InterlockedCompareExchange(&session->state, 0, 1) != 1)
	{
		errno = ENOENT;
		return -1;
	}

	steady_timer *timer = static_cast<steady_timer *>(session->timer_handle);
	if (timer)
		timer->cancel();
	return 0;
}

void comm_sleep_op::cancel_all(CommunicatorImpl *impl)
{
	if (!impl)
		return;

	AcquireSRWLockExclusive(&impl->sleep_lock);
	struct list_head *pos;
	list_for_each(pos, &impl->live_sleeps)
	{
		comm_sleep_op *op = list_entry(pos, comm_sleep_op, live_list_);
		SleepSession *session = op->session_;
		if (session)
			::InterlockedExchange(&session->state, 2);
		op->timer_.cancel();
	}
	ReleaseSRWLockExclusive(&impl->sleep_lock);
}

int comm_sleep_op::start(CommunicatorImpl *impl, SleepSession *session,
						 long long ms)
{
	if (!impl || !session)
	{
		errno = EINVAL;
		return -1;
	}
	if (::InterlockedCompareExchange(&impl->shutting_down, 0, 0) != 0)
	{
		errno = ECANCELED;
		return -1;
	}
	if (::InterlockedCompareExchange(&session->state, 1, 0) != 0)
	{
		errno = EALREADY;
		return -1;
	}
	void *mem = malloc(sizeof(comm_sleep_op));
	if (!mem)
	{
		errno = ENOMEM;
		::InterlockedExchange(&session->state, 0);
		return -1;
	}

	comm_sleep_op *op = new (mem) comm_sleep_op();
	op->impl_ = impl;
	op->session_ = session;
	if (op->timer_.init(executor(impl->kernel.get_io_context())) != 0)
	{
		int error = errno;
		op->~comm_sleep_op();
		free(op);
		errno = error;
		::InterlockedExchange(&session->state, 0);
		return -1;
	}

	op->timer_.expires_after(std::chrono::milliseconds(
		ms > INT_MAX ? INT_MAX : (int)ms));

	session->timer_handle = &op->timer_;
	AcquireSRWLockExclusive(&impl->sleep_lock);
	list_add_tail(&op->live_list_, &impl->live_sleeps);
	ReleaseSRWLockExclusive(&impl->sleep_lock);

	composed_op_set_executor(op, executor(impl->kernel.get_io_context()));
	composed_op_add_ref(op);
	int ret = op->timer_.async_wait(&comm_sleep_op::timer_cb, op,
								   composed_op_destroy);
	if (ret < 0)
	{
		int error = errno;
		session->timer_handle = nullptr;
		AcquireSRWLockExclusive(&impl->sleep_lock);
		if (!list_empty(&op->live_list_))
		{
			list_del(&op->live_list_);
			INIT_LIST_HEAD(&op->live_list_);
		}
		ReleaseSRWLockExclusive(&impl->sleep_lock);
		/* No wait was submitted.  Drop the child reference and make the
		 * final release destroy the object without synthesising a callback. */
		composed_op_release(op);
		::InterlockedExchange(&op->handler_completed_, 1);
		composed_op_release(op);
		errno = error;
		::InterlockedExchange(&session->state, 0);
		return -1;
	}

	composed_op_release(op);
	return 0;
}

