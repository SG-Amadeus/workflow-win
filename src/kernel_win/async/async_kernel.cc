#include "async_kernel.h"

#include <errno.h>
#include <process.h>
#include <stdlib.h>

async_kernel::async_kernel()
	: worker_threads_(nullptr), worker_size_(0), worker_capacity_(0),
	  op_pools_(nullptr), worker_count_(0), initialized_(false),
	  started_(false), work_owned_(false)
{
	::InitializeCriticalSection(&worker_mutex_);
}

async_kernel::~async_kernel()
{
	this->deinit();
	::DeleteCriticalSection(&worker_mutex_);
}

int async_kernel::init(size_t io_threads)
{
	if (started_)
	{
		errno = EALREADY;
		return -1;
	}

	if (io_.init() != 0)
		return -1;
	initialized_ = true;

	op_pools_ = op_pools_create();
	if (!op_pools_)
	{
		int error = errno ? errno : ENOMEM;
		this->deinit();
		errno = error;
		return -1;
	}
	io_.set_op_pools(op_pools_);
	thrdpool_t *blocking_pool = thrdpool_create(1, 0);
	if (!blocking_pool)
	{
		int error = errno ? errno : ENOMEM;
		this->deinit();
		errno = error;
		return -1;
	}
	io_.set_blocking_pool(blocking_pool);
	/* Keep run() alive while the kernel is started, matching an ASIO
	 * executor_work_guard owned by the Async kernel. */
	io_.work_started();
	work_owned_ = true;

	size_t n = io_threads ? io_threads : 1;
	EnterCriticalSection(&worker_mutex_);
	for (size_t i = 0; i < n; ++i)
	{
		if (this->add_worker_locked() != 0)
		{
			int error = errno ? errno : ENOMEM;
			LeaveCriticalSection(&worker_mutex_);
			this->deinit();
			errno = error;
			return -1;
		}
	}
	LeaveCriticalSection(&worker_mutex_);
	started_ = true;
	return 0;
}

void async_kernel::deinit()
{
	/* Blocking tasks must retire while IOCP workers are still able to process
	 * their completion posts. */
	io_.destroy_blocking_pool();
	if (work_owned_)
	{
		/* The kernel guard is the one remaining work item.  Every operation
		 * cancelled by the owner must retire before workers are stopped. */
		(void)io_.wait_for_work(1);
		io_.work_finished();
		work_owned_ = false;
	}
	this->destroy_workers();
	if (initialized_)
	{
		io_.shutdown();
		initialized_ = false;
	}
	started_ = false;
	io_.set_op_pools(nullptr);
	if (op_pools_)
	{
		op_pools_destroy(op_pools_);
		op_pools_ = nullptr;
	}
}

io_context &async_kernel::get_io_context()
{
	return io_;
}

unsigned __stdcall async_kernel::io_worker(void *ctx)
{
	async_kernel *self = static_cast<async_kernel *>(ctx);
	for (;;)
	{
		int rc = self->io_.run_one();
		if (rc == -2)
			return 0;
		if (rc < 0 || self->io_.stopped())
			return 0;
	}
}

int async_kernel::add_worker_locked()
{
	if (worker_size_ == worker_capacity_)
	{
		size_t capacity = worker_capacity_ ? worker_capacity_ * 2 : 4;
		HANDLE *threads = static_cast<HANDLE *>(
			::realloc(worker_threads_, capacity * sizeof(*threads)));
		if (!threads)
		{
			errno = ENOMEM;
			return -1;
		}
		worker_threads_ = threads;
		worker_capacity_ = capacity;
	}

	uintptr_t thread = ::_beginthreadex(nullptr, 0,
			&async_kernel::io_worker, this, 0, nullptr);
	if (!thread)
	{
		errno = errno ? errno : EAGAIN;
		return -1;
	}

	worker_threads_[worker_size_++] = reinterpret_cast<HANDLE>(thread);
	++worker_count_;
	return 0;
}

void async_kernel::reap_workers_locked()
{
	size_t i = 0;
	while (i < worker_size_)
	{
		if (::WaitForSingleObject(worker_threads_[i], 0) != WAIT_OBJECT_0)
		{
			++i;
			continue;
		}

		::CloseHandle(worker_threads_[i]);
		worker_threads_[i] = worker_threads_[worker_size_ - 1];
		--worker_size_;
	}
}

void async_kernel::destroy_workers()
{
	EnterCriticalSection(&worker_mutex_);
	this->reap_workers_locked();
	size_t workers = worker_size_;
	if (!workers)
	{
		worker_count_ = 0;
		LeaveCriticalSection(&worker_mutex_);
		return;
	}
	LeaveCriticalSection(&worker_mutex_);

	this->io_.stop();
	/* ASIO run() threads are owned by the caller. Wake every thread that may
	 * still be blocked in GetQueuedCompletionStatus, then join them. */
	for (size_t i = 0; i < workers; ++i)
		(void)this->io_.request_worker_exit();

	EnterCriticalSection(&worker_mutex_);
	for (size_t i = 0; i < worker_size_; ++i)
	{
		::WaitForSingleObject(worker_threads_[i], INFINITE);
		::CloseHandle(worker_threads_[i]);
	}
	::free(worker_threads_);
	worker_threads_ = nullptr;
	worker_size_ = 0;
	worker_capacity_ = 0;
	worker_count_ = 0;
	LeaveCriticalSection(&worker_mutex_);
}

int async_kernel::increase_worker()
{
	if (!started_)
	{
		errno = EINVAL;
		return -1;
	}

	EnterCriticalSection(&worker_mutex_);
	this->reap_workers_locked();
	int rc = this->add_worker_locked();
	LeaveCriticalSection(&worker_mutex_);
	return rc;
}

int async_kernel::decrease_worker()
{
	if (!started_)
	{
		errno = EINVAL;
		return -1;
	}

	EnterCriticalSection(&worker_mutex_);
	this->reap_workers_locked();
	if (worker_count_ <= 1)
	{
		LeaveCriticalSection(&worker_mutex_);
		errno = EBUSY;
		return -1;
	}
	--worker_count_;
	LeaveCriticalSection(&worker_mutex_);

	if (io_.request_worker_exit() != 0)
	{
		EnterCriticalSection(&worker_mutex_);
		++worker_count_;
		LeaveCriticalSection(&worker_mutex_);
		return -1;
	}
	return 0;
}

int async_kernel::is_worker_thread() const
{
	return io_.running_in_this_thread() ? 1 : 0;
}

