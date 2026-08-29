#include "steady_timer.h"
#include "service/timer_queue.h"
#include "op/timer_wait_op.h"

#include <cerrno>
#include <cstdlib>
#include <new>
#include <windows.h>

class steady_timer::impl
{
public:
	executor executor_;
	CRITICAL_SECTION mutex_;
	std::chrono::steady_clock::time_point expiry;
	bool might_have_pending_waits;
	timer_per_timer_data timer_data;
	volatile LONG refs_;

	explicit impl(executor ex)
		: executor_(ex),
		  expiry(std::chrono::steady_clock::now()),
		  might_have_pending_waits(false)
	{
		::InitializeCriticalSection(&mutex_);
		timer_per_timer_data_init(&timer_data);
		refs_ = 1;
	}

	~impl()
	{
		::DeleteCriticalSection(&mutex_);
	}

	void acquire()
	{
		::InterlockedIncrement(&refs_);
	}

	void release()
	{
		if (::InterlockedDecrement(&refs_) == 0)
		{
			this->~impl();
			free(this);
		}
	}
};

void steady_timer_impl_release(steady_timer::impl *impl)
{
	if (impl)
		impl->release();
}

steady_timer::steady_timer()
	: impl_(nullptr)
{
}

steady_timer *steady_timer::create(executor ex)
{
	void *mem = malloc(sizeof(steady_timer));
	if (!mem)
	{
		errno = ENOMEM;
		return nullptr;
	}

	steady_timer *timer = new (mem) steady_timer();
	if (timer->init(ex) == 0)
		return timer;

	int error = errno;
	timer->~steady_timer();
	free(timer);
	errno = error;
	return nullptr;
}

void steady_timer::destroy(steady_timer *timer)
{
	if (timer)
	{
		timer->~steady_timer();
		free(timer);
	}
}

int steady_timer::init(executor ex)
{
	if (impl_)
	{
		errno = EALREADY;
		return -1;
	}
	if (!ex.context())
	{
		errno = EINVAL;
		return -1;
	}
	void *mem = malloc(sizeof(impl));
	if (mem)
		impl_ = new (mem) impl(ex);
	else
	{
		errno = ENOMEM;
		return -1;
	}
	return 0;
}

steady_timer::~steady_timer()
{
	if (impl_)
	{
		this->cancel();
		impl_->release();
	}
}

size_t steady_timer::expires_after(std::chrono::milliseconds ms)
{
	if (!impl_)
	{
		errno = EINVAL;
		return 0;
	}
	return expires_at(std::chrono::steady_clock::now() + ms);
}

size_t steady_timer::expires_at(std::chrono::steady_clock::time_point tp)
{
	if (!impl_)
	{
		errno = EINVAL;
		return 0;
	}
	io_context *io = impl_->executor_.context();
	size_t count = 0;

	{
		::EnterCriticalSection(&impl_->mutex_);
		if (io && impl_->might_have_pending_waits)
		{
			count = io->cancel_timer(&io->timer_queue_,
									 &impl_->timer_data);
			impl_->might_have_pending_waits = false;
		}
		impl_->expiry = tp;
		::LeaveCriticalSection(&impl_->mutex_);
	}

	return count;
}

size_t steady_timer::cancel()
{
	if (!impl_)
	{
		errno = EINVAL;
		return 0;
	}
	io_context *io = impl_->executor_.context();
	size_t count = 0;

	{
		::EnterCriticalSection(&impl_->mutex_);
		if (io && impl_->might_have_pending_waits)
		{
			count = io->cancel_timer(&io->timer_queue_,
									 &impl_->timer_data);
			impl_->might_have_pending_waits = false;
		}
		::LeaveCriticalSection(&impl_->mutex_);
	}

	return count;
}

executor steady_timer::get_executor() const
{
	return impl_ ? impl_->executor_ : executor();
}

int steady_timer::async_wait(void (*callback)(void *, async_error_code), void *context)
{
	return this->async_wait(callback, context, nullptr);
}

int steady_timer::async_wait(void (*callback)(void *, async_error_code), void *context,
							 void (*destroy)(void *))
{
	if (!impl_)
	{
		errno = EINVAL;
		return -1;
	}
	if (!callback)
	{
		errno = EINVAL;
		return -1;
	}

	io_context *io = impl_->executor_.context();
	if (!io)
	{
		errno = EINVAL;
		return -1;
	}

	steady_timer_wait_op *op =
		timer_wait_op_alloc(io->get_op_pools());
	if (!op)
	{
		errno = ENOMEM;
		return -1;
	}
	op->error.clear();
	op->cancellation_key = nullptr;
	op->work_ = handler_work(impl_->executor_);
	op->callback_ = callback;
	op->context_ = context;
	op->destroy_ = destroy;
	op->impl_ = impl_;
	impl_->acquire();

	::EnterCriticalSection(&impl_->mutex_);
	io->work_started();
	bool scheduled = io->schedule_timer(&io->timer_queue_, impl_->expiry,
								 &impl_->timer_data, op);
	if (scheduled)
		impl_->might_have_pending_waits = true;
	::LeaveCriticalSection(&impl_->mutex_);
	if (!scheduled)
	{
		/* schedule_timer only fails on heap allocation failure; in that case
		 * the op has not been queued. */
		io->work_finished();
		timer_wait_op_free(op);
		errno = ENOMEM;
		return -1;
	}

	return 0;
}

