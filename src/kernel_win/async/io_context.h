/*
  AsyncCore: IOCP io_context (C with class, no templates).

  io_context is the replacement for asio::detail::win_iocp_io_context.
  It does not know about handlers or concrete operations: it only queues
  win_iocp_operation objects and calls op->complete(owner, ec, bytes).

  All post/dispatch/defer convenience lives in strand, which creates
  executor_op and submits it through the op-based interface below.
*/

#ifndef _ASYNC_IO_CONTEXT_H_
#define _ASYNC_IO_CONTEXT_H_

#include <WinSock2.h>
#include <Windows.h>
#include <stddef.h>

#include "../thrdpool.h"
#include "op/win_iocp_operation.h"
#include "op/op_queue.h"
#include "service/timer_queue.h"
#include "service/timer_queue_set.h"
#include "service/socket_service.h"
#include "service/handle_service.h"
class strand;
class op_pools;

class io_context
{
	friend class steady_timer;

public:
	io_context();
	~io_context();

private:
	io_context(const io_context &);
	io_context &operator=(const io_context &);

public:
	

	/* Event loop.  Upper layer calls run() from its own worker threads. */
	int run();
	int run_one();
	int poll();
	int poll_one();

	/* Lifecycle. */
	/* Explicit initialization for the no-exception Workflow interface. */
	int init();
	int stop();
	bool stopped() const;
	void restart();
	int shutdown();

	/* Work tracking. */
	void work_started();
	void work_finished();
	/* Wait until all work except the caller-owned guard remains.  This is used
	 * by async_kernel during quiescent shutdown while IOCP workers are alive. */
	int wait_for_work(LONG keep_work);

	/* Async kernel worker management.  The wake is not user work: it only
	 * releases one worker that is blocked in GetQueuedCompletionStatus. */
	int request_worker_exit();
	int take_worker_exit();

	/* Function-level scheduling, used by upper Workflow layers. */
	int post(void (*routine)(void *), void *context);
	int post(void (*routine)(void *), void *context,
			 void (*destroy)(void *));
	int dispatch(void (*routine)(void *), void *context);
	int dispatch(void (*routine)(void *), void *context,
				 void (*destroy)(void *));
	int defer(void (*routine)(void *), void *context);
	int defer(void (*routine)(void *), void *context,
			  void (*destroy)(void *));

	/* Workflow's blocking file adaptation.  The task runs on the kernel-owned
	 * blocking pool and its completion is responsible for posting back to this
	 * io_context. */
	int post_blocking(void (*routine)(void *), void *context,
					 void (*abandon)(void *));
	void set_blocking_pool(thrdpool_t *pool);
	void destroy_blocking_pool();

	/* Returns true when called from one of this io_context's run() threads. */
	bool running_in_this_thread() const;

	/* ASIO can_dispatch(): true when the current thread is inside run(). */
	bool can_dispatch() const;

	/* Register an external handle (socket/file) with this IOCP. */
	int register_handle(HANDLE handle);

	/* Pools belong to the Async kernel, not to the IOCP dispatcher itself. */
	void set_op_pools(op_pools *pools);
	op_pools *get_op_pools() const;

	/* ASIO service objects: they own the open-handle registry. */
	socket_service &get_socket_service();
	handle_service &get_handle_service();

	/* Op-based scheduling interface, matching ASIO internals. */
	void post_immediate_completion(win_iocp_operation *op, bool is_continuation);
	void post_deferred_completion(win_iocp_operation *op);
	void post_deferred_completions(op_queue &ops);

	/* Called by I/O operations. */
	void on_pending(win_iocp_operation *op);
	void on_completion(win_iocp_operation *op, async_error_code error,
					   size_t bytes);

	/* Timer queue, matching ASIO timer_scheduler surface. */
	bool schedule_timer(timer_queue *q,
						const std::chrono::steady_clock::time_point &time,
						timer_per_timer_data *timer, wait_op *op);
	size_t cancel_timer(timer_queue *q, timer_per_timer_data *timer,
						size_t max_cancelled = (size_t)-1);
	void cancel_timer_by_key(timer_queue *q, timer_per_timer_data *timer,
							 void *key);
	void move_timer(timer_queue *q, timer_per_timer_data *target,
					timer_per_timer_data *source);

	/* Shutdown helper: destroy all remaining ops in the queue. */
	void destroy_operations(op_queue &ops);

private:
	HANDLE iocp_;
	LONG outstanding_work_;
	LONG stopped_;
	LONG stop_event_posted_;
	LONG shutdown_;
	LONG dispatch_required_;
	LONG worker_exit_requests_;
	DWORD gqcs_timeout_;
	CRITICAL_SECTION dispatch_mutex_;
	op_queue completed_ops_;
	timer_queue_set timer_queues_;
	timer_queue timer_queue_;
	HANDLE waitable_timer_;
	HANDLE timer_thread_;
	HANDLE work_event_;
	socket_service socket_service_;
	handle_service handle_service_;
	op_pools *op_pools_;
	thrdpool_t *blocking_pool_;

	int do_one(DWORD msec);
	void post_deferred_completions_locked(op_queue &ops);
	void destroy_timers();
	bool ensure_timer_thread_locked();
	void update_timer_wait_locked();
	void wake_timer_thread();
	static unsigned __stdcall timer_thread_routine(void *context);
	static void blocking_pending(const struct thrdpool_task *task);
	static void blocking_run(void *context);
};

#endif /* _ASYNC_IO_CONTEXT_H_ */

