/*
  AsyncCore: io_context implementation.
  Non-template C with class port of asio::detail::win_iocp_io_context.
*/

#include "io_context.h"
#include "op/executor_op.h"
#include "op/op_pools.h"
#include "error.h"

#include <windows.h>
#include <errno.h>

#include <chrono>
#include <climits>
#include <new>
#include <process.h>

#include "../list.h"

namespace
{
	const ULONG_PTR wake_for_dispatch = 1;
	const ULONG_PTR overlapped_contains_result = 2;
	const ULONG_PTR wake_for_worker_exit = 3;
	const long timer_max_timeout_usec = 5 * 60 * 1000 * 1000L;
	const LONG timer_period_msec = 5 * 60 * 1000;

	thread_local io_context *current_io_context = nullptr;

	struct current_io_context_guard
	{
		io_context *io_context_;
		io_context *prev_;

		explicit current_io_context_guard(io_context *io)
			: io_context_(io),
			  prev_(current_io_context)
		{
			current_io_context = io_context_;
		}

		~current_io_context_guard()
		{
			current_io_context = prev_;
		}
	};

	struct blocking_task_context
	{
		io_context *io;
		void (*routine)(void *);
		void (*abandon)(void *);
		void *context;
	};
}

io_context::io_context()
	: iocp_(nullptr),
	  outstanding_work_(0),
	  stopped_(0),
	  stop_event_posted_(0),
	  shutdown_(0),
	  dispatch_required_(0),
	  worker_exit_requests_(0),
	  gqcs_timeout_(INFINITE),
	  socket_service_(this),
	  handle_service_(this),
	  op_pools_(nullptr),
	  blocking_pool_(nullptr),
	  waitable_timer_(nullptr),
	  timer_thread_(nullptr),
	  work_event_(nullptr)
{
	::InitializeCriticalSection(&dispatch_mutex_);
	timer_queue_set_init(&timer_queues_);
	timer_queue_init(&timer_queue_);
	timer_queue_set_insert(&timer_queues_, &timer_queue_);
}

void io_context::set_op_pools(op_pools *pools)
{
	op_pools_ = pools;
}

op_pools *io_context::get_op_pools() const
{
	return op_pools_;
}

socket_service &io_context::get_socket_service()
{
	return socket_service_;
}

handle_service &io_context::get_handle_service()
{
	return handle_service_;
}

io_context::~io_context()
{
	this->destroy_blocking_pool();
	if (iocp_)
		this->shutdown();

	if (iocp_)
		::CloseHandle(iocp_);
	if (work_event_)
		::CloseHandle(work_event_);

	timer_queue_set_erase(&timer_queues_, &timer_queue_);
	timer_queue_destroy(&timer_queue_);
	if (waitable_timer_)
		::CloseHandle(waitable_timer_);
	::DeleteCriticalSection(&dispatch_mutex_);
}

int io_context::init()
{
	if (iocp_)
	{
		errno = EALREADY;
		return -1;
	}

	iocp_ = ::CreateIoCompletionPort(
		INVALID_HANDLE_VALUE, nullptr, 0, 0);
	if (!iocp_)
	{
		errno = async_win_error_to_errno((int)::GetLastError());
		return -1;
	}
	work_event_ = ::CreateEventW(nullptr, TRUE, TRUE, nullptr);
	if (!work_event_)
	{
		int error = async_win_error_to_errno((int)::GetLastError());
		::CloseHandle(iocp_);
		iocp_ = nullptr;
		errno = error;
		return -1;
	}

	::InterlockedExchange(&shutdown_, 0);
	::InterlockedExchange(&stopped_, 0);
	::InterlockedExchange(&stop_event_posted_, 0);
	::InterlockedExchange(&dispatch_required_, 0);
	::InterlockedExchange(&worker_exit_requests_, 0);

	return 0;
}

int io_context::run()
{
	if (!iocp_)
	{
		errno = EINVAL;
		return -1;
	}
	if (::InterlockedExchangeAdd(&outstanding_work_, 0) == 0)
	{
		if (this->stop() != 0)
			return -1;
		return 0;
	}

	current_io_context_guard guard(this);

	int n = 0;
	while (!this->stopped()
		&& ::InterlockedExchangeAdd(&shutdown_, 0) == 0)
	{
		errno = 0;
		int rc = this->do_one(INFINITE);
		if (rc < 0)
			return -1;
		if (rc > 0)
			++n;
		/* A user handler may leave errno set; it must not stop the loop. */
		errno = 0;
	}

	return n;
}

int io_context::run_one()
{
	if (!iocp_)
	{
		errno = EINVAL;
		return -1;
	}
	if (::InterlockedExchangeAdd(&outstanding_work_, 0) == 0)
	{
		if (this->stop() != 0)
			return -1;
		return 0;
	}

	current_io_context_guard guard(this);

	errno = 0;
	int rc = this->do_one(INFINITE);
	if (rc == -2)
		return -2;
	if (rc < 0)
		return -1;
	errno = 0;
	return rc > 0 ? 1 : 0;
}

int io_context::poll()
{
	if (!iocp_)
	{
		errno = EINVAL;
		return -1;
	}
	if (::InterlockedExchangeAdd(&outstanding_work_, 0) == 0)
	{
		if (this->stop() != 0)
			return -1;
		return 0;
	}

	current_io_context_guard guard(this);

	int n = 0;
	for (;;)
	{
		errno = 0;
		int rc = this->do_one(0);
		if (rc < 0)
			return -1;
		if (rc == 0)
			break;
		++n;
		errno = 0;
	}

	return n;
}

int io_context::poll_one()
{
	if (!iocp_)
	{
		errno = EINVAL;
		return -1;
	}
	if (::InterlockedExchangeAdd(&outstanding_work_, 0) == 0)
	{
		if (this->stop() != 0)
			return -1;
		return 0;
	}

	current_io_context_guard guard(this);

	errno = 0;
	int rc = this->do_one(0);
	if (rc < 0)
		return -1;
	errno = 0;
	return rc > 0 ? 1 : 0;
}

int io_context::stop()
{
	errno = 0;
	if (!iocp_)
	{
		errno = EINVAL;
		return -1;
	}

	if (::InterlockedExchange(&stopped_, 1) == 0)
	{
		if (::InterlockedExchange(&stop_event_posted_, 1) == 0)
		{
			if (!::PostQueuedCompletionStatus(iocp_, 0, 0, nullptr))
			{
				errno = async_win_error_to_errno((int)::GetLastError());
				return -1;
			}
		}
	}

	return 0;
}

bool io_context::stopped() const
{
	return ::InterlockedExchangeAdd(
		const_cast<volatile LONG *>(&stopped_), 0) != 0;
}

void io_context::restart()
{
	::InterlockedExchange(&stopped_, 0);
	::InterlockedExchange(&stop_event_posted_, 0);
}

int io_context::shutdown()
{
	if (!iocp_)
		return 0;
	if (this->running_in_this_thread())
	{
		errno = EBUSY;
		return -1;
	}

	::InterlockedExchange(&shutdown_, 1);
	if (this->stop() != 0)
		return -1;
	this->wake_timer_thread();
	if (timer_thread_)
	{
		::WaitForSingleObject(timer_thread_, INFINITE);
		::CloseHandle(timer_thread_);
		timer_thread_ = nullptr;
	}

	/* ASIO services own the open-handle registry.  Close them before draining
	 * so pending operations complete with cancellation and can be retired. */
	this->socket_service_.shutdown();
	this->handle_service_.shutdown();

	while (::InterlockedExchangeAdd(&outstanding_work_, 0) > 0)
	{
		op_queue ops;

		this->destroy_timers();

		EnterCriticalSection(&dispatch_mutex_);
		ops.push(completed_ops_);
		LeaveCriticalSection(&dispatch_mutex_);

		if (!ops.empty())
		{
			this->destroy_operations(ops);
			continue;
		}

		DWORD bytes_transferred = 0;
		ULONG_PTR completion_key = 0;
		OVERLAPPED *overlapped = nullptr;
		::GetQueuedCompletionStatus(iocp_, &bytes_transferred,
			&completion_key, &overlapped, gqcs_timeout_);

		if (overlapped)
		{
			::InterlockedDecrement(&outstanding_work_);
			win_iocp_operation_from_overlapped(overlapped)->destroy();
		}
		else
		{
			::Sleep(0);
		}
	}

	return 0;
}

void io_context::work_started()
{
	::InterlockedIncrement(&outstanding_work_);
	if (work_event_)
		::ResetEvent(work_event_);
}

void io_context::work_finished()
{
	LONG remaining = ::InterlockedDecrement(&outstanding_work_);
	if (remaining <= 1 && work_event_)
		::SetEvent(work_event_);
	if (remaining == 0)
		this->stop();
}

int io_context::wait_for_work(LONG keep_work)
{
	if (!iocp_ || !work_event_ || keep_work < 0)
	{
		errno = EINVAL;
		return -1;
	}
	if (this->running_in_this_thread())
	{
		errno = EBUSY;
		return -1;
	}

	for (;;)
	{
		if (::InterlockedExchangeAdd(&outstanding_work_, 0) <= keep_work)
			return 0;
		DWORD result = ::WaitForSingleObject(work_event_, INFINITE);
		if (result != WAIT_OBJECT_0)
		{
			errno = async_win_error_to_errno((int)::GetLastError());
			return -1;
		}
	}
}

int io_context::request_worker_exit()
{
	if (!iocp_ || ::InterlockedExchangeAdd(&shutdown_, 0) != 0)
	{
		errno = !iocp_ ? EINVAL : ECANCELED;
		return -1;
	}

	::InterlockedIncrement(&worker_exit_requests_);
	if (!::PostQueuedCompletionStatus(iocp_, 0, wake_for_worker_exit,
									  nullptr))
	{
		::InterlockedDecrement(&worker_exit_requests_);
		errno = async_win_error_to_errno((int)::GetLastError());
		return -1;
	}
	return 0;
}

int io_context::take_worker_exit()
{
	LONG old;

	for (;;)
	{
		old = ::InterlockedExchangeAdd(&worker_exit_requests_, 0);
		if (old == 0)
			return 0;
		if (::InterlockedCompareExchange(&worker_exit_requests_, old - 1,
										 old) == old)
			return 1;
	}
}

int io_context::post(void (*routine)(void *), void *context)
{
	return this->post(routine, context, nullptr);
}

int io_context::post(void (*routine)(void *), void *context,
					 void (*destroy)(void *))
{
	if (!routine || !iocp_ ||
		::InterlockedExchangeAdd(&shutdown_, 0) != 0)
	{
		errno = !routine || !iocp_ ? EINVAL : ECANCELED;
		return -1;
	}

	executor_op *op = op_pools_alloc_executor(op_pools_);
	if (!op)
	{
		errno = ENOMEM;
		return -1;
	}

	new (op) executor_op(routine, context, destroy, op_pools_);
	this->post_immediate_completion(op, false);
	return 0;
}

int io_context::dispatch(void (*routine)(void *), void *context)
{
	return this->dispatch(routine, context, nullptr);
}

int io_context::dispatch(void (*routine)(void *), void *context,
						 void (*destroy)(void *))
{
	if (!routine || !iocp_)
	{
		errno = EINVAL;
		return -1;
	}
	if (::InterlockedExchangeAdd(&shutdown_, 0) != 0)
	{
		errno = ECANCELED;
		return -1;
	}
	if (this->running_in_this_thread())
	{
		routine(context);
		return 0;
	}

	return this->post(routine, context, destroy);
}

int io_context::defer(void (*routine)(void *), void *context)
{
	return this->defer(routine, context, nullptr);
}

int io_context::defer(void (*routine)(void *), void *context,
					  void (*destroy)(void *))
{
	if (!routine || !iocp_ ||
		::InterlockedExchangeAdd(&shutdown_, 0) != 0)
	{
		errno = !routine || !iocp_ ? EINVAL : ECANCELED;
		return -1;
	}

	executor_op *op = op_pools_alloc_executor(op_pools_);
	if (!op)
	{
		errno = ENOMEM;
		return -1;
	}

	new (op) executor_op(routine, context, destroy, op_pools_);
	this->post_immediate_completion(op, true);
	return 0;
}

void io_context::set_blocking_pool(thrdpool_t *pool)
{
	blocking_pool_ = pool;
}

void io_context::destroy_blocking_pool()
{
	if (blocking_pool_)
	{
		thrdpool_destroy(&io_context::blocking_pending, blocking_pool_);
		blocking_pool_ = nullptr;
	}
}

void io_context::blocking_run(void *context)
{
	blocking_task_context *task =
		static_cast<blocking_task_context *>(context);
	task->routine(task->context);
	task->io->work_finished();
	free(task);
}

void io_context::blocking_pending(const struct thrdpool_task *task)
{
	blocking_task_context *pending =
		static_cast<blocking_task_context *>(task->context);
	if (pending->abandon)
		pending->abandon(pending->context);
	pending->io->work_finished();
	free(pending);
}

int io_context::post_blocking(void (*routine)(void *), void *context,
							 void (*abandon)(void *))
{
	if (!routine || !blocking_pool_ || !iocp_ ||
		::InterlockedExchangeAdd(&shutdown_, 0) != 0)
	{
		errno = !routine || !iocp_ ? EINVAL :
			(::InterlockedExchangeAdd(&shutdown_, 0) != 0 ? ECANCELED : ENODEV);
		return -1;
	}

	blocking_task_context *task =
		static_cast<blocking_task_context *>(malloc(sizeof *task));
	if (!task)
	{
		errno = ENOMEM;
		return -1;
	}
	task->io = this;
	task->routine = routine;
	task->abandon = abandon;
	task->context = context;
	this->work_started();
	struct thrdpool_task pool_task = { &io_context::blocking_run, task };
	if (thrdpool_schedule(&pool_task, blocking_pool_) != 0)
	{
		int error = errno ? errno : EIO;
		this->work_finished();
		free(task);
		errno = error;
		return -1;
	}
	return 0;
}

bool io_context::running_in_this_thread() const
{
	return current_io_context == this;
}

bool io_context::can_dispatch() const
{
	return current_io_context == this;
}

int io_context::register_handle(HANDLE handle)
{
	if (!iocp_ || !handle || handle == INVALID_HANDLE_VALUE ||
		::InterlockedExchangeAdd(&shutdown_, 0) != 0)
	{
		errno = !iocp_ || !handle || handle == INVALID_HANDLE_VALUE
			? EINVAL : ECANCELED;
		return -1;
	}

	if (::CreateIoCompletionPort(handle, iocp_, 0, 0) == 0)
	{
		errno = async_win_error_to_errno((int)::GetLastError());
		return -1;
	}

	return 0;
}

void io_context::post_immediate_completion(win_iocp_operation *op, bool)
{
	this->work_started();
	this->post_deferred_completion(op);
}

void io_context::post_deferred_completion(win_iocp_operation *op)
{
	EnterCriticalSection(&dispatch_mutex_);
	op_queue ops;
	ops.push(op);
	this->post_deferred_completions_locked(ops);
	LeaveCriticalSection(&dispatch_mutex_);
}

void io_context::post_deferred_completions(op_queue &ops)
{
	EnterCriticalSection(&dispatch_mutex_);
	this->post_deferred_completions_locked(ops);
	LeaveCriticalSection(&dispatch_mutex_);
}

void io_context::post_deferred_completions_locked(op_queue &ops)
{
	while (!ops.empty())
	{
		win_iocp_operation *op = ops.front();
		ops.pop();
		op->ready_ = 1;

		if (!::PostQueuedCompletionStatus(iocp_, 0, 0, op))
		{
			/* The op that failed to post must be queued too. */
			completed_ops_.push(op);

			while (!ops.empty())
			{
				win_iocp_operation *remaining = ops.front();
				ops.pop();
				remaining->ready_ = 1;
				completed_ops_.push(remaining);
			}

			::InterlockedExchange(&dispatch_required_, 1);
			break;
		}
	}

	/* ASIO retries the fallback queue when a worker next acquires dispatch
	 * responsibility.  No second completion packet is required here: the
	 * current worker is already at the responsibility hand-off point. */
}

bool io_context::schedule_timer(timer_queue *q,
								const std::chrono::steady_clock::time_point &time,
								timer_per_timer_data *timer, wait_op *op)
{
	int rc;
	bool wake_failed = false;
	DWORD wake_error = 0;
	EnterCriticalSection(&dispatch_mutex_);
	rc = timer_queue_enqueue(q, time, timer, op);
	if (rc >= 0 && !this->ensure_timer_thread_locked())
	{
		wake_failed = true;
		wake_error = ERROR_NOT_ENOUGH_MEMORY;
		op_queue rollback;
		timer_queue_cancel_timer(q, timer, rollback, 1);
		rollback.pop();
	}
	else if (rc >= 0)
		this->update_timer_wait_locked();
	LeaveCriticalSection(&dispatch_mutex_);

	if (rc < 0 || wake_failed)
	{
		if (wake_failed)
			errno = async_win_error_to_errno((int)wake_error);
		return false;
	}

	return true;
}

size_t io_context::cancel_timer(timer_queue *q, timer_per_timer_data *timer,
								size_t max_cancelled)
{
	op_queue ops;

	size_t count = 0;
	EnterCriticalSection(&dispatch_mutex_);
	count = timer_queue_cancel_timer(q, timer, ops, max_cancelled);
	this->update_timer_wait_locked();
	LeaveCriticalSection(&dispatch_mutex_);

	if (!ops.empty())
		this->post_deferred_completions(ops);
	return count;
}

void io_context::cancel_timer_by_key(timer_queue *q,
									 timer_per_timer_data *timer, void *key)
{
	op_queue ops;

	EnterCriticalSection(&dispatch_mutex_);
	timer_queue_cancel_timer_by_key(q, timer, ops, key);
	this->update_timer_wait_locked();
	LeaveCriticalSection(&dispatch_mutex_);

	if (!ops.empty())
		this->post_deferred_completions(ops);
}

void io_context::move_timer(timer_queue *q, timer_per_timer_data *target,
							timer_per_timer_data *source)
{
	EnterCriticalSection(&dispatch_mutex_);
	timer_queue_move_timer(q, target, source);
	this->update_timer_wait_locked();
	LeaveCriticalSection(&dispatch_mutex_);
}

void io_context::destroy_timers()
{
	op_queue ops;

	EnterCriticalSection(&dispatch_mutex_);
	timer_queue_set_get_all_timers(&timer_queues_, ops);
	LeaveCriticalSection(&dispatch_mutex_);

	if (!ops.empty())
		this->destroy_operations(ops);
}

bool io_context::ensure_timer_thread_locked()
{
	if (timer_thread_)
		return true;

	waitable_timer_ = ::CreateWaitableTimerW(nullptr, FALSE, nullptr);
	if (!waitable_timer_)
		return false;

	uintptr_t thread = ::_beginthreadex(nullptr, 0,
		&io_context::timer_thread_routine, this, 0, nullptr);
	if (!thread)
	{
		::CloseHandle(waitable_timer_);
		waitable_timer_ = nullptr;
		return false;
	}

	timer_thread_ = reinterpret_cast<HANDLE>(thread);
	this->update_timer_wait_locked();
	return true;
}

void io_context::update_timer_wait_locked()
{
	if (!waitable_timer_ || ::InterlockedExchangeAdd(&shutdown_, 0) != 0)
		return;

	long usec = timer_queue_set_wait_duration_usec(&timer_queues_,
											 5 * 60 * 1000 * 1000L);
	if (usec < 1)
		usec = 1;

	LARGE_INTEGER due;
	due.QuadPart = -(LONGLONG)usec * 10;
	::SetWaitableTimer(waitable_timer_, &due, 5 * 60 * 1000,
		nullptr, nullptr, FALSE);
}

void io_context::wake_timer_thread()
{
	if (!waitable_timer_)
		return;

	LARGE_INTEGER due;
	due.QuadPart = -1;
	::SetWaitableTimer(waitable_timer_, &due, 0,
		nullptr, nullptr, FALSE);
}

unsigned __stdcall io_context::timer_thread_routine(void *context)
{
	io_context *self = static_cast<io_context *>(context);
	while (::InterlockedExchangeAdd(&self->shutdown_, 0) == 0)
	{
		DWORD result = ::WaitForSingleObject(self->waitable_timer_, INFINITE);
		if (result != WAIT_OBJECT_0 ||
			::InterlockedExchangeAdd(&self->shutdown_, 0) != 0)
			break;

		::InterlockedExchange(&self->dispatch_required_, 1);
		if (!::PostQueuedCompletionStatus(self->iocp_, 0,
			wake_for_dispatch, nullptr))
			break;
	}
	return 0;
}

void io_context::on_pending(win_iocp_operation *op)
{
	if (::InterlockedCompareExchange(&op->ready_, 1, 0) == 1)
	{
		if (!::PostQueuedCompletionStatus(
				iocp_, 0, overlapped_contains_result, op))
		{
			EnterCriticalSection(&dispatch_mutex_);
			completed_ops_.push(op);
			::InterlockedExchange(&dispatch_required_, 1);
			LeaveCriticalSection(&dispatch_mutex_);
		}
	}
}

void io_context::on_completion(win_iocp_operation *op,
							   async_error_code error, size_t bytes)
{
	op->ready_ = 1;
	op->Internal = reinterpret_cast<ULONG_PTR>(&error.category());
	op->Offset = error.value();
	op->OffsetHigh = (DWORD)bytes;

	if (!::PostQueuedCompletionStatus(
			iocp_, 0, overlapped_contains_result, op))
	{
		EnterCriticalSection(&dispatch_mutex_);
		completed_ops_.push(op);
		::InterlockedExchange(&dispatch_required_, 1);
		LeaveCriticalSection(&dispatch_mutex_);
	}
}

void io_context::destroy_operations(op_queue &ops)
{
	while (!ops.empty())
	{
		win_iocp_operation *op = ops.front();
		ops.pop();
		::InterlockedDecrement(&outstanding_work_);
		op->destroy();
	}
}

int io_context::do_one(DWORD msec)
{
	for (;;)
	{
		if (::InterlockedCompareExchange(
				&dispatch_required_, 0, 1) == 1)
		{
			op_queue ops;

			EnterCriticalSection(&dispatch_mutex_);
			ops.push(completed_ops_);
			timer_queue_set_get_ready_timers(&timer_queues_, ops);
			this->post_deferred_completions_locked(ops);
			this->update_timer_wait_locked();
			LeaveCriticalSection(&dispatch_mutex_);
		}

		DWORD bytes_transferred = 0;
		ULONG_PTR completion_key = 0;
		OVERLAPPED *overlapped = nullptr;

		::SetLastError(0);
		DWORD timeout = (msec < gqcs_timeout_) ? msec : gqcs_timeout_;
		BOOL ok = ::GetQueuedCompletionStatus(iocp_,
			&bytes_transferred, &completion_key, &overlapped, timeout);
		DWORD last_error = ::GetLastError();

		if (overlapped)
		{
			win_iocp_operation *op = win_iocp_operation_from_overlapped(overlapped);
			async_error_code error;
			size_t bytes;

			if (completion_key == overlapped_contains_result)
			{
				const std::error_category *category =
					reinterpret_cast<const std::error_category *>(op->Internal);
				error = async_error_code(static_cast<int>(op->Offset),
					*category);
				bytes = (size_t)op->OffsetHigh;
			}
			else
			{
				/* A successful GetQueuedCompletionStatus does not carry an
				 * error.  GetLastError is meaningful only when the call failed.
				 * Internal timer/strand completions use the normal completion
				 * key, so passing a stale thread error here corrupts composed
				 * operation cancellation and reference accounting. */
				error = ok ? async_error_code() : async_native_error(last_error);
				bytes = (size_t)bytes_transferred;

				op->Internal = reinterpret_cast<ULONG_PTR>(&error.category());
				op->Offset = error.value();
				op->OffsetHigh = (DWORD)bytes;
			}

			if (::InterlockedCompareExchange(&op->ready_, 1, 0) == 1)
			{
				op->complete(this, error, bytes);
				this->work_finished();
				return 1;
			}
		}
		else if (!ok)
		{
			if (last_error != WAIT_TIMEOUT)
			{
				errno = async_win_error_to_errno((int)last_error);
				return -1;
			}

			if (msec == INFINITE)
				continue;

			return 0;
		}
		else if (completion_key == wake_for_dispatch)
		{
			/* Woken to dispatch completed_ops_. */
		}
		else if (completion_key == wake_for_worker_exit)
		{
			if (this->take_worker_exit())
				return -2;
		}
		else
		{
			::InterlockedExchange(&stop_event_posted_, 0);

			if (::InterlockedExchangeAdd(&stopped_, 0) != 0)
			{
				if (::InterlockedExchange(
						&stop_event_posted_, 1) == 0)
				{
					::PostQueuedCompletionStatus(
						iocp_, 0, 0, nullptr);
				}

				return 0;
			}
		}
	}
}

