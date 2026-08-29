/*
  AsyncCore: minimal non-template executor, named after ASIO.

  executor is the simplified equivalent of asio::any_io_executor for the
  two executor kinds used by the upper Workflow layer:
    - io_context
    - strand

  Like asio::strand's executor, a strand executor owns a reference to the
  shared strand implementation.  The io_context itself remains externally
  owned by the upper event-loop lifecycle.
*/

#ifndef _ASYNC_EXECUTOR_H_
#define _ASYNC_EXECUTOR_H_

#include "io_context.h"

class strand;

class executor
{
public:
	executor();
	explicit executor(io_context &io);
	explicit executor(strand &s);
	executor(const executor &other);
	executor &operator=(const executor &other);
	~executor();

	int dispatch(void (*routine)(void *), void *context);
	int dispatch(void (*routine)(void *), void *context,
				 void (*destroy)(void *));
	int post(void (*routine)(void *), void *context);
	int post(void (*routine)(void *), void *context,
			 void (*destroy)(void *));
	int defer(void (*routine)(void *), void *context);
	int defer(void (*routine)(void *), void *context,
			  void (*destroy)(void *));

	bool running_in_this_thread() const;
	bool is_io_context() const;
	bool is_strand() const;
	bool operator==(const executor &other) const;
	bool operator!=(const executor &other) const;

	io_context *context() const;

private:
	enum kind
	{
		EXECUTOR_NONE = 0,
		EXECUTOR_IO_CONTEXT,
		EXECUTOR_STRAND
	};

	kind kind_;
	union
	{
		io_context *io_context_;
		void *strand_impl_;
	} u_;

	void acquire();
	void release();
};

class executor_work_guard
{
public:
	executor_work_guard();
	explicit executor_work_guard(executor ex);
	~executor_work_guard();

	executor_work_guard(const executor_work_guard &) = delete;
	executor_work_guard &operator=(const executor_work_guard &) = delete;

	executor get_executor() const;
	bool owns_work() const;
	void set_executor(executor ex);
	void reset();

private:
	executor executor_;
	io_context *io_context_;
	bool owns_work_;
};

#endif /* _ASYNC_EXECUTOR_H_ */

