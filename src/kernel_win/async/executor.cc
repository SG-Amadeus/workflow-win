#include "executor.h"
#include "strand.h"

#include <errno.h>

executor::executor()
	: kind_(EXECUTOR_NONE)
{
	u_.io_context_ = nullptr;
}

executor::executor(io_context &io)
	: kind_(EXECUTOR_IO_CONTEXT)
{
	u_.io_context_ = &io;
}

executor::executor(strand &s)
	: kind_(EXECUTOR_STRAND)
{
	u_.strand_impl_ = strand::executor_acquire(s);
	if (!u_.strand_impl_)
		kind_ = EXECUTOR_NONE;
}

executor::executor(const executor &other)
	: kind_(other.kind_), u_(other.u_)
{
	this->acquire();
}

executor &executor::operator=(const executor &other)
{
	if (this != &other)
	{
		this->release();
		kind_ = other.kind_;
		u_ = other.u_;
		this->acquire();
	}
	return *this;
}

executor::~executor()
{
	this->release();
}

void executor::acquire()
{
	if (kind_ == EXECUTOR_STRAND && u_.strand_impl_)
		strand::executor_acquire_impl(u_.strand_impl_);
}

void executor::release()
{
	if (kind_ == EXECUTOR_STRAND && u_.strand_impl_)
		strand::executor_release(u_.strand_impl_);
	kind_ = EXECUTOR_NONE;
	u_.io_context_ = nullptr;
}

int executor::dispatch(void (*routine)(void *), void *context)
{
	return this->dispatch(routine, context, nullptr);
}

int executor::dispatch(void (*routine)(void *), void *context,
					   void (*destroy)(void *))
{
	switch (kind_)
	{
	case EXECUTOR_IO_CONTEXT:
		return u_.io_context_->dispatch(routine, context, destroy);
	case EXECUTOR_STRAND:
		return strand::executor_dispatch(
			u_.strand_impl_, routine, context, destroy);
	default:
		errno = EINVAL;
		return -1;
	}
}

int executor::post(void (*routine)(void *), void *context)
{
	return this->post(routine, context, nullptr);
}

int executor::post(void (*routine)(void *), void *context,
				   void (*destroy)(void *))
{
	switch (kind_)
	{
	case EXECUTOR_IO_CONTEXT:
		return u_.io_context_->post(routine, context, destroy);
	case EXECUTOR_STRAND:
		return strand::executor_post(
			u_.strand_impl_, routine, context, destroy);
	default:
		errno = EINVAL;
		return -1;
	}
}

int executor::defer(void (*routine)(void *), void *context)
{
	return this->defer(routine, context, nullptr);
}

int executor::defer(void (*routine)(void *), void *context,
					void (*destroy)(void *))
{
	switch (kind_)
	{
	case EXECUTOR_IO_CONTEXT:
		return u_.io_context_->defer(routine, context, destroy);
	case EXECUTOR_STRAND:
		return strand::executor_defer(
			u_.strand_impl_, routine, context, destroy);
	default:
		errno = EINVAL;
		return -1;
	}
}

bool executor::running_in_this_thread() const
{
	switch (kind_)
	{
	case EXECUTOR_IO_CONTEXT:
		return u_.io_context_->running_in_this_thread();
	case EXECUTOR_STRAND:
		return strand::executor_running(u_.strand_impl_);
	default:
		return false;
	}
}

bool executor::is_io_context() const
{
	return kind_ == EXECUTOR_IO_CONTEXT;
}

bool executor::is_strand() const
{
	return kind_ == EXECUTOR_STRAND;
}

bool executor::operator==(const executor &other) const
{
	if (kind_ != other.kind_)
		return false;

	switch (kind_)
	{
	case EXECUTOR_IO_CONTEXT:
		return u_.io_context_ == other.u_.io_context_;
	case EXECUTOR_STRAND:
		return u_.strand_impl_ == other.u_.strand_impl_;
	default:
		return true;
	}
}

bool executor::operator!=(const executor &other) const
{
	return !(*this == other);
}

io_context *executor::context() const
{
	switch (kind_)
	{
	case EXECUTOR_IO_CONTEXT:
		return u_.io_context_;
	case EXECUTOR_STRAND:
		return strand::executor_context(u_.strand_impl_);
	default:
		return nullptr;
	}
}

executor_work_guard::executor_work_guard()
	: executor_(), io_context_(nullptr), owns_work_(false)
{
}

executor_work_guard::executor_work_guard(executor ex)
	: executor_(ex),
	  io_context_(ex.context()),
	  owns_work_(io_context_ != nullptr)
{
	if (owns_work_)
		io_context_->work_started();
}

executor_work_guard::~executor_work_guard()
{
	this->reset();
}

executor executor_work_guard::get_executor() const
{
	return executor_;
}

bool executor_work_guard::owns_work() const
{
	return owns_work_;
}

void executor_work_guard::set_executor(executor ex)
{
	this->reset();
	executor_ = ex;
	io_context_ = ex.context();
	owns_work_ = io_context_ != nullptr;
	if (owns_work_)
		io_context_->work_started();
}

void executor_work_guard::reset()
{
	if (owns_work_)
	{
		owns_work_ = false;
		if (io_context_)
			io_context_->work_finished();
	}
}

