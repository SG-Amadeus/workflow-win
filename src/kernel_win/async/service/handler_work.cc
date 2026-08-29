#include "handler_work.h"


#include "../io_context.h"

handler_work::handler_work()
	: executor_(), owns_work_(false)
{
}

handler_work::handler_work(const handler_work &other)
	: executor_(other.executor_), owns_work_(other.owns_work_)
{
	if (owns_work_)
		this->acquire_work();
}

handler_work::handler_work(handler_work &&other)
	: executor_(other.executor_), owns_work_(other.owns_work_)
{
	other.executor_ = executor();
	other.owns_work_ = false;
}

handler_work &handler_work::operator=(const handler_work &other)
{
	if (this != &other)
	{
		this->release_work();
		executor_ = other.executor_;
		owns_work_ = other.owns_work_;
		if (owns_work_)
			this->acquire_work();
	}
	return *this;
}

handler_work &handler_work::operator=(handler_work &&other)
{
	if (this != &other)
	{
		this->release_work();
		executor_ = other.executor_;
		owns_work_ = other.owns_work_;
		other.executor_ = executor();
		other.owns_work_ = false;
	}
	return *this;
}

handler_work::handler_work(executor ex)
	: executor_(ex), owns_work_(executor_.is_strand())
{
	this->acquire_work();
}

handler_work::~handler_work()
{
	this->release_work();
}

void handler_work::acquire_work()
{
	if (owns_work_ && executor_.context())
		executor_.context()->work_started();
}

void handler_work::release_work()
{
	if (owns_work_)
	{
		if (executor_.context())
			executor_.context()->work_finished();
		owns_work_ = false;
	}
}

int handler_work::complete(void (*routine)(void *), void *context,
						   void (*destroy)(void *))
{
	return executor_.dispatch(routine, context, destroy);
}

int handler_work::post(void (*routine)(void *), void *context,
					   void (*destroy)(void *))
{
	return executor_.post(routine, context, destroy);
}

int handler_work::defer(void (*routine)(void *), void *context,
						void (*destroy)(void *))
{
	return executor_.defer(routine, context, destroy);
}

executor handler_work::get_executor() const
{
	return executor_;
}

bool handler_work::owns_work() const
{
	return owns_work_;
}

void handler_work::reset()
{
	this->release_work();
	executor_ = executor();
	owns_work_ = false;
}

