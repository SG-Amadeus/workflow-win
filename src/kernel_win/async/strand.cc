/*
  AsyncCore: strand implementation.
  Non-template C with class port of asio::detail::strand_service.
*/

#include "strand.h"
#include "op/executor_op.h"
#include "op/op_pools.h"

#include <windows.h>
#include <errno.h>
#include <new>
#include <stdlib.h>

#include "op/op_queue.h"

class strand::impl : public win_iocp_operation
{
	public:
	io_context *io_context_;
	CRITICAL_SECTION mutex_;
	LONG locked_;
	op_queue waiting_;
	op_queue ready_;
	LONG refcount_;

	explicit impl(io_context *io)
		: win_iocp_operation(&strand::do_complete),
		  io_context_(io),
		  locked_(0),
		  refcount_(1)
	{
		::InitializeCriticalSection(&mutex_);
	}

	~impl()
	{
		::DeleteCriticalSection(&mutex_);
	}

	void acquire()
	{
		::InterlockedIncrement(&refcount_);
	}

	void release()
	{
		if (::InterlockedDecrement(&refcount_) == 0)
		{
			this->~impl();
			free(this);
		}
	}
};

namespace
{

class strand_call_frame
{
public:
	void *impl;
	strand_call_frame *prev;
};

thread_local strand_call_frame *current_frame = nullptr;

} /* namespace */

strand::strand()
	: impl_(nullptr)
{
}

strand *strand::create(io_context *io)
{
	void *mem = malloc(sizeof(strand));
	if (!mem)
	{
		errno = ENOMEM;
		return nullptr;
	}

	strand *s = new (mem) strand();
	if (s->init(io) == 0)
		return s;

	int error = errno;
	s->~strand();
	free(s);
	errno = error;
	return nullptr;
}

void strand::destroy(strand *s)
{
	if (s)
	{
		s->~strand();
		free(s);
	}
}

int strand::init(io_context *io)
{
	if (impl_)
	{
		errno = EALREADY;
		return -1;
	}
	if (!io)
	{
		errno = EINVAL;
		return -1;
	}

	void *mem = malloc(sizeof(impl));
	if (mem)
		impl_ = new (mem) impl(io);
	else
	{
		errno = ENOMEM;
		return -1;
	}
	return 0;
}

strand::strand(const strand &other)
	: impl_(other.impl_)
{
	if (impl_)
		impl_->acquire();
}

strand &strand::operator=(const strand &other)
{
	if (this != &other)
	{
		if (other.impl_)
			other.impl_->acquire();
		if (impl_)
			impl_->release();
		impl_ = other.impl_;
	}

	return *this;
}

strand::~strand()
{
	if (impl_)
		impl_->release();
}

void *strand::executor_acquire(const strand &s)
{
	if (s.impl_)
		s.impl_->acquire();
	return s.impl_;
}

void strand::executor_acquire_impl(void *p)
{
	if (p)
		static_cast<impl *>(p)->acquire();
}

void strand::executor_release(void *p)
{
	if (p)
		static_cast<impl *>(p)->release();
}

int strand::executor_dispatch(void *p, void (*routine)(void *),
							  void *context, void (*destroy)(void *))
{
	executor_acquire_impl(p);
	strand alias;
	alias.impl_ = static_cast<impl *>(p);
	int ret = alias.dispatch(routine, context, destroy);
	alias.impl_ = nullptr;
	executor_release(p);
	return ret;
}

int strand::executor_post(void *p, void (*routine)(void *),
						  void *context, void (*destroy)(void *))
{
	strand alias;
	alias.impl_ = static_cast<impl *>(p);
	int ret = alias.post(routine, context, destroy);
	alias.impl_ = nullptr;
	return ret;
}

int strand::executor_defer(void *p, void (*routine)(void *),
						   void *context, void (*destroy)(void *))
{
	strand alias;
	alias.impl_ = static_cast<impl *>(p);
	int ret = alias.defer(routine, context, destroy);
	alias.impl_ = nullptr;
	return ret;
}

bool strand::executor_running(void *p)
{
	for (strand_call_frame *frame = current_frame; frame; frame = frame->prev)
	{
		if (frame->impl == p)
			return true;
	}
	return false;
}

io_context *strand::executor_context(void *p)
{
	impl *i = static_cast<impl *>(p);
	return i ? i->io_context_ : nullptr;
}

bool strand::running_in_this_thread() const
{
	for (strand_call_frame *frame = current_frame; frame; frame = frame->prev)
	{
		if (frame->impl == impl_)
			return true;
	}
	return false;
}

bool strand::operator==(const strand &other) const
{
	return impl_ == other.impl_;
}

bool strand::operator!=(const strand &other) const
{
	return !(*this == other);
}

int strand::dispatch(void (*routine)(void *), void *context)
{
	return this->dispatch(routine, context, nullptr);
}

int strand::dispatch(void (*routine)(void *), void *context,
					 void (*destroy)(void *))
{
	if (!impl_ || !routine)
	{
		errno = EINVAL;
		return -1;
	}
	if (this->running_in_this_thread())
	{
		if (routine)
			routine(context);
		return 0;
	}

	op_pools *pools = impl_->io_context_->get_op_pools();
	executor_op *op = op_pools_alloc_executor(pools);
	if (!op)
	{
		errno = ENOMEM;
		return -1;
	}

	new (op) executor_op(routine, context, destroy, pools);
	this->dispatch_op(op);
	return 0;
}

int strand::post(void (*routine)(void *), void *context)
{
	return this->post(routine, context, nullptr);
}

int strand::post(void (*routine)(void *), void *context,
				 void (*destroy)(void *))
{
	if (!impl_ || !routine)
	{
		errno = EINVAL;
		return -1;
	}
	op_pools *pools = impl_->io_context_->get_op_pools();
	executor_op *op = op_pools_alloc_executor(pools);
	if (!op)
	{
		errno = ENOMEM;
		return -1;
	}

	new (op) executor_op(routine, context, destroy, pools);
	this->post_op(op);
	return 0;
}

int strand::defer(void (*routine)(void *), void *context)
{
	return this->defer(routine, context, nullptr);
}

int strand::defer(void (*routine)(void *), void *context,
				  void (*destroy)(void *))
{
	if (!impl_ || !routine)
	{
		errno = EINVAL;
		return -1;
	}
	op_pools *pools = impl_->io_context_->get_op_pools();
	executor_op *op = op_pools_alloc_executor(pools);
	if (!op)
	{
		errno = ENOMEM;
		return -1;
	}

	new (op) executor_op(routine, context, destroy, pools);
	this->defer_op(op);
	return 0;
}

void strand::dispatch_op(win_iocp_operation *op)
{
	if (!impl_)
	{
		op->destroy();
		return;
	}
	if (this->running_in_this_thread())
	{
		op->complete(impl_->io_context_, async_error_code(), 0);
		return;
	}

	bool can_dispatch = impl_->io_context_->can_dispatch();

	EnterCriticalSection(&impl_->mutex_);

	if (can_dispatch && !impl_->locked_)
	{
		impl_->locked_ = true;
		LeaveCriticalSection(&impl_->mutex_);

		strand_call_frame frame = { impl_, current_frame };
		current_frame = &frame;
		op->complete(impl_->io_context_, async_error_code(), 0);

		EnterCriticalSection(&impl_->mutex_);
		impl_->ready_.push(impl_->waiting_);

		bool more = (impl_->locked_ = !impl_->ready_.empty()) != 0;
		LeaveCriticalSection(&impl_->mutex_);

		current_frame = frame.prev;

		if (more)
		{
			impl_->acquire();
			impl_->io_context_->post_immediate_completion(impl_, false);
		}

		return;
	}

	if (impl_->locked_)
	{
		impl_->waiting_.push(op);
		LeaveCriticalSection(&impl_->mutex_);
	}
	else
	{
		impl_->locked_ = true;
		LeaveCriticalSection(&impl_->mutex_);

		impl_->ready_.push(op);
		impl_->acquire();
		impl_->io_context_->post_immediate_completion(impl_, false);
	}
}

void strand::post_op(win_iocp_operation *op)
{
	if (!impl_)
	{
		op->destroy();
		return;
	}
	EnterCriticalSection(&impl_->mutex_);

	if (impl_->locked_)
	{
		impl_->waiting_.push(op);
		LeaveCriticalSection(&impl_->mutex_);
	}
	else
	{
		impl_->locked_ = true;
		LeaveCriticalSection(&impl_->mutex_);

		impl_->ready_.push(op);
		impl_->acquire();
		impl_->io_context_->post_immediate_completion(impl_, false);
	}
}

void strand::defer_op(win_iocp_operation *op)
{
	if (!impl_)
	{
		op->destroy();
		return;
	}
	EnterCriticalSection(&impl_->mutex_);

	if (impl_->locked_)
	{
		impl_->waiting_.push(op);
		LeaveCriticalSection(&impl_->mutex_);
	}
	else
	{
		impl_->locked_ = true;
		LeaveCriticalSection(&impl_->mutex_);

		impl_->ready_.push(op);
		impl_->acquire();
		impl_->io_context_->post_immediate_completion(impl_, true);
	}
}

void strand::do_complete(void *owner, win_iocp_operation *base,
		async_error_code /*error*/, size_t /*bytes*/)
{
	strand::impl *impl = static_cast<strand::impl *>(base);

	if (!owner)
	{
		/* Shutdown/destroy path: destroy queued work, then release the
		 * wakeup reference held by this queued strand operation. */
		while (!impl->ready_.empty())
		{
			win_iocp_operation *op = impl->ready_.front();
			impl->ready_.pop();
			op->destroy();
		}

		EnterCriticalSection(&impl->mutex_);
		while (!impl->waiting_.empty())
		{
			win_iocp_operation *op = impl->waiting_.front();
			impl->waiting_.pop();
			op->destroy();
		}
		LeaveCriticalSection(&impl->mutex_);

		impl->release();
		return;
	}

	io_context *io = static_cast<io_context *>(owner);

	strand_call_frame frame = { impl, current_frame };
	current_frame = &frame;

	/* Run all ready ops. The ready queue is only touched inside the strand. */
	while (!impl->ready_.empty())
	{
		win_iocp_operation *op = impl->ready_.front();
		impl->ready_.pop();
		op->complete(io, async_error_code(), 0);
	}

	/* Move waiting ops to ready and reschedule if any remain. */
	EnterCriticalSection(&impl->mutex_);
	impl->ready_.push(impl->waiting_);

	bool more = (impl->locked_ = !impl->ready_.empty()) != 0;
	LeaveCriticalSection(&impl->mutex_);

	current_frame = frame.prev;

	if (more)
	{
		impl->acquire();
		io->post_immediate_completion(base, true);
	}

	impl->release();
}

