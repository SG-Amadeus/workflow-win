/*
  AsyncCore: non-template strand (C with class).

  This is the C with class equivalent of asio::strand<io_context::executor_type>.
  Internally it is built on op_queue and executor_op.

  The strand is reference counted and copyable, like ASIO.  A queued IOCP
  wakeup holds one reference, so destroying the last user strand does not
  destroy work that is already scheduled.
*/

#ifndef _ASYNC_STRAND_H_
#define _ASYNC_STRAND_H_

#include "io_context.h"

class strand
{
public:
	strand();
	strand(const strand &other);
	strand &operator=(const strand &other);
	~strand();
	static strand *create(io_context *io);
	static void destroy(strand *s);
	int init(io_context *io);

	/* Op-based interface: enqueue an op that already carries its own data. */
	void dispatch_op(win_iocp_operation *op);
	void post_op(win_iocp_operation *op);
	void defer_op(win_iocp_operation *op);

	/* Function-level interface, used by upper Workflow layers. */
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
	bool operator==(const strand &other) const;
	bool operator!=(const strand &other) const;

private:
	class impl;
	friend class executor;

	impl *impl_;

	static void *executor_acquire(const strand &s);
	static void executor_acquire_impl(void *p);
	static void executor_release(void *p);
	static int executor_dispatch(void *p, void (*routine)(void *),
			void *context, void (*destroy)(void *));
	static int executor_post(void *p, void (*routine)(void *),
			void *context, void (*destroy)(void *));
	static int executor_defer(void *p, void (*routine)(void *),
			void *context, void (*destroy)(void *));
	static bool executor_running(void *p);
	static io_context *executor_context(void *p);

	static void do_complete(void *owner, win_iocp_operation *base,
			async_error_code error, size_t bytes);
};

#endif /* _ASYNC_STRAND_H_ */

