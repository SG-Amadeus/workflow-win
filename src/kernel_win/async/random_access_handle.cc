#include "random_access_handle.h"
#include "op/handle_read_op.h"
#include "op/handle_write_op.h"
#include "op/handle_readv_op.h"
#include "op/handle_writev_op.h"
#include "op/op_pools.h"
#include "op/async_handler.h"
#include "service/handle_service.h"
#include "io_context.h"
#include "error.h"
#include <windows.h>
#include <errno.h>
#include <new>
#include <stdlib.h>
#include <string.h>
#include "../list.h"
class random_access_handle::impl : public handle_impl
{
public:
	executor executor_;
	CRITICAL_SECTION mutex_;
	CRITICAL_SECTION blocking_mutex_;
	volatile LONG refs_;
	bool blocking_;
	explicit impl(executor ex)
		: handle_impl(),
		  executor_(ex),
		  blocking_(false)
	{
		::InitializeCriticalSection(&mutex_);
		::InitializeCriticalSection(&blocking_mutex_);
		refs_ = 1;
	}
	~impl()
	{
		::DeleteCriticalSection(&blocking_mutex_);
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
namespace
{

static bool handle_is_overlapped(HANDLE handle)
{
	struct io_status_block
	{
		union
		{
			NTSTATUS status;
			void *pointer;
		};
		ULONG_PTR information;
	};
	struct file_mode_information
	{
		ULONG mode;
	};
	typedef NTSTATUS (NTAPI *query_information_file_fn)(
		HANDLE, io_status_block *, void *, ULONG, int);
	query_information_file_fn query =
		(query_information_file_fn)::GetProcAddress(
			::GetModuleHandleW(L"ntdll.dll"), "NtQueryInformationFile");
	if (!query)
		return false;

	io_status_block status;
	file_mode_information mode;
	memset(&status, 0, sizeof status);
	memset(&mode, 0, sizeof mode);
	if (query(handle, &status, &mode, sizeof mode, 16) != 0)
		return false;

	return (mode.mode & 0x30) == 0;
}

class flush_call
{
public:
	io_context *io;
	executor executor_;
	HANDLE handle;
	void (*callback)(void *, async_error_code, size_t);
	void (*destroy)(void *);
	void *context;
	async_error_code error;
	flush_call(io_context *io_, executor ex, HANDLE h,
			   void (*cb)(void *, async_error_code, size_t), void *ctx,
			   void (*dtor)(void *))
		: io(io_), executor_(ex), handle(h), callback(cb), destroy(dtor),
		  context(ctx), error()
	{
	}
};
static void flush_call_free(flush_call *call)
{
	call->~flush_call();
	free(call);
}
static void flush_call_destroy(void *context)
{
	flush_call *call = static_cast<flush_call *>(context);
	if (call->destroy)
		call->destroy(call->context);
	flush_call_free(call);
}
static void flush_call_complete(void *context)
{
	flush_call *call = static_cast<flush_call *>(context);
	call->callback(call->context, call->error, 0);
	flush_call_free(call);
}
static void flush_call_abandon(void *context)
{
	flush_call *call = static_cast<flush_call *>(context);
	if (call->handle != INVALID_HANDLE_VALUE)
	{
		::CloseHandle(call->handle);
		call->handle = INVALID_HANDLE_VALUE;
	}
	flush_call_destroy(call);
}
static void flush_call_run(void *context)
{
	flush_call *call = static_cast<flush_call *>(context);
	BOOL ok = ::FlushFileBuffers(call->handle);
	call->error = ok ? async_error_code() :
		async_native_error(::GetLastError());
	::CloseHandle(call->handle);
	call->handle = INVALID_HANDLE_VALUE;
	if (call->executor_.post(&flush_call_complete, call,
						 &flush_call_destroy) != 0)
	{
		flush_call_destroy(call);
	}
}

enum blocking_file_operation
{
	blocking_file_read,
	blocking_file_write,
	blocking_file_readv,
	blocking_file_writev
};

class blocking_file_call
{
public:
	random_access_handle::impl *impl_;
	executor executor_;
	HANDLE handle_;
	blocking_file_operation operation_;
	uint64_t offset_;
	void *buffer_;
	const struct iovec *iov_;
	int iovcnt_;
	size_t size_;
	void (*callback_)(void *, async_error_code, size_t);
	void (*destroy_)(void *);
	void *context_;
	async_error_code error_;
	size_t bytes_;
};

static void blocking_file_call_free(blocking_file_call *call)
{
	if (call->impl_)
	{
		call->impl_->release();
		call->impl_ = nullptr;
	}
	call->~blocking_file_call();
	free(call);
}

static void blocking_file_call_destroy(void *context)
{
	blocking_file_call *call = static_cast<blocking_file_call *>(context);
	if (call->destroy_)
		call->destroy_(call->context_);
	blocking_file_call_free(call);
}

static void blocking_file_call_complete(void *context)
{
	blocking_file_call *call = static_cast<blocking_file_call *>(context);
	call->callback_(call->context_, call->error_, call->bytes_);
	blocking_file_call_free(call);
}

static void blocking_file_call_run(void *context)
{
	blocking_file_call *call = static_cast<blocking_file_call *>(context);
	DWORD transferred = 0;
	LARGE_INTEGER offset;
	offset.QuadPart = (LONGLONG)call->offset_;

	::EnterCriticalSection(&call->impl_->blocking_mutex_);
	if (!::SetFilePointerEx(call->handle_, offset, nullptr, FILE_BEGIN))
		call->error_ = async_native_error(::GetLastError());
	else if (call->operation_ == blocking_file_read ||
			 call->operation_ == blocking_file_write)
	{
		BOOL ok = call->operation_ == blocking_file_read
			? ::ReadFile(call->handle_, call->buffer_, (DWORD)call->size_,
						 &transferred, nullptr)
			: ::WriteFile(call->handle_, call->buffer_, (DWORD)call->size_,
						  &transferred, nullptr);
		if (!ok)
			call->error_ = async_native_error(::GetLastError());
		else
			call->bytes_ = transferred;
	}
	else
	{
		for (int i = 0; i < call->iovcnt_; ++i)
		{
			const struct iovec &v = call->iov_[i];
			if (v.iov_len > ULONG_MAX)
			{
				call->error_ = async_native_error(ERROR_ARITHMETIC_OVERFLOW);
				break;
			}
			BOOL ok = call->operation_ == blocking_file_readv
				? ::ReadFile(call->handle_, v.iov_base, (DWORD)v.iov_len,
							 &transferred, nullptr)
				: ::WriteFile(call->handle_, v.iov_base, (DWORD)v.iov_len,
							  &transferred, nullptr);
			if (!ok)
			{
				call->error_ = async_native_error(::GetLastError());
				break;
			}
			call->bytes_ += transferred;
			if (transferred != v.iov_len)
				break;
		}
	}
	::LeaveCriticalSection(&call->impl_->blocking_mutex_);

	if (call->executor_.post(&blocking_file_call_complete, call,
							 &blocking_file_call_destroy) != 0)
		blocking_file_call_destroy(call);
}

static int blocking_file_start(random_access_handle::impl *impl,
							   blocking_file_operation operation,
							   uint64_t offset, void *buffer, size_t size,
							   const struct iovec *iov, int iovcnt,
							   void (*callback)(void *, async_error_code, size_t),
							   void *context, void (*destroy)(void *))
{
	if (size > ULONG_MAX || iovcnt < 0)
	{
		errno = EINVAL;
		return -1;
	}
	void *mem = malloc(sizeof(blocking_file_call));
	if (!mem)
	{
		errno = ENOMEM;
		return -1;
	}
	blocking_file_call *call = new (mem) blocking_file_call();
	call->impl_ = impl;
	impl->acquire();
	call->executor_ = impl->executor_;
	EnterCriticalSection(&impl->mutex_);
	call->handle_ = impl->handle_;
	LeaveCriticalSection(&impl->mutex_);
	if (call->handle_ == INVALID_HANDLE_VALUE)
	{
		blocking_file_call_free(call);
		errno = EBADF;
		return -1;
	}
	call->operation_ = operation;
	call->offset_ = offset;
	call->buffer_ = buffer;
	call->iov_ = iov;
	call->iovcnt_ = iovcnt;
	call->size_ = size;
	call->callback_ = callback;
	call->destroy_ = destroy;
	call->context_ = context;
	call->error_.clear();
	call->bytes_ = 0;
	if (call->executor_.context()->post_blocking(
			&blocking_file_call_run, call, &blocking_file_call_destroy) != 0)
	{
		blocking_file_call_free(call);
		return -1;
	}
	return 0;
}
} /* namespace */
random_access_handle::random_access_handle()
	: impl_(nullptr)
{
}
random_access_handle *random_access_handle::create(executor ex)
{
	void *mem = malloc(sizeof(random_access_handle));
	if (!mem)
	{
		errno = ENOMEM;
		return nullptr;
	}
	random_access_handle *handle = new (mem) random_access_handle();
	if (handle->init(ex) == 0)
		return handle;
	int error = errno;
	handle->~random_access_handle();
	free(handle);
	errno = error;
	return nullptr;
}
void random_access_handle::destroy(random_access_handle *handle)
{
	if (handle)
	{
		handle->~random_access_handle();
		free(handle);
	}
}
int random_access_handle::init(executor ex)
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
random_access_handle::~random_access_handle()
{
	if (impl_)
	{
		this->close();
		impl_->release();
	}
}
int random_access_handle::assign(HANDLE handle)

{
	bool overlapped = handle_is_overlapped(handle);
	return this->assign(handle, overlapped);
}

int random_access_handle::assign(HANDLE handle, bool overlapped)
{
	if (!impl_)
	{
		errno = EINVAL;
		return -1;
	}
	if (!handle || handle == INVALID_HANDLE_VALUE)
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
	EnterCriticalSection(&impl_->mutex_);
	if (impl_->handle_ != INVALID_HANDLE_VALUE)
	{
		LeaveCriticalSection(&impl_->mutex_);
		errno = EBUSY;
		return -1;
	}
	LeaveCriticalSection(&impl_->mutex_);
	if (io->get_handle_service().register_handle(impl_, handle,
									 overlapped) != 0)
	{
		EnterCriticalSection(&impl_->mutex_);
		LeaveCriticalSection(&impl_->mutex_);
		return -1;
	}
	impl_->blocking_ = !overlapped;
	return 0;
}
int random_access_handle::async_read_some_at(
	uint64_t offset, void *buf, size_t size,
	void (*callback)(void *, async_error_code, size_t), void *context)
{
	return this->async_read_some_at(offset, buf, size, callback, context,
									nullptr);
}
int random_access_handle::async_read_some_at(uint64_t offset,
											 void *buf, size_t size,
											 void (*callback)(void *, async_error_code, size_t),
											 void *context,
											 void (*destroy)(void *))
{
	if (!impl_)
	{
		errno = EINVAL;
		return -1;
	}
	if (!callback || !buf || size == 0 || size > ULONG_MAX)
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
	EnterCriticalSection(&impl_->mutex_);
	bool blocking = impl_->blocking_;
	bool assigned = impl_->handle_ != INVALID_HANDLE_VALUE;
	LeaveCriticalSection(&impl_->mutex_);
	if (!assigned)
	{
		errno = EBADF;
		return -1;
	}
	if (blocking)
		return blocking_file_start(impl_, blocking_file_read, offset, buf,
								 size, nullptr, 0, callback, context, destroy);
	handle_read_op *op = handle_read_op_alloc(io->get_op_pools());
	if (!op)
	{
		errno = ENOMEM;
		return -1;
	}
	op->work_ = handler_work(impl_->executor_);
	op->callback_ = callback;
	op->context_ = context;
	op->destroy_ = destroy;
	EnterCriticalSection(&impl_->mutex_);
	if (impl_->handle_ == INVALID_HANDLE_VALUE)
	{
		LeaveCriticalSection(&impl_->mutex_);
	handle_read_op_free(op);
		errno = EBADF;
		return -1;
	}
	HANDLE handle = impl_->handle_;
	LeaveCriticalSection(&impl_->mutex_);
	io->get_handle_service().start_read_op(impl_, offset, buf, size, op);
	return 0;
}
int random_access_handle::async_write_some_at(
	uint64_t offset, const void *buf, size_t size,
	void (*callback)(void *, async_error_code, size_t), void *context)
{
	return this->async_write_some_at(offset, buf, size, callback, context,
									 nullptr);
}
int random_access_handle::async_write_some_at(uint64_t offset,
											  const void *buf, size_t size,
											  void (*callback)(void *, async_error_code, size_t),
											  void *context,
											  void (*destroy)(void *))
{
	if (!impl_)
	{
		errno = EINVAL;
		return -1;
	}
	if (!callback || !buf || size == 0 || size > ULONG_MAX)
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
	EnterCriticalSection(&impl_->mutex_);
	bool blocking = impl_->blocking_;
	bool assigned = impl_->handle_ != INVALID_HANDLE_VALUE;
	LeaveCriticalSection(&impl_->mutex_);
	if (!assigned)
	{
		errno = EBADF;
		return -1;
	}
	if (blocking)
		return blocking_file_start(impl_, blocking_file_write, offset,
								 const_cast<void *>(buf), size, nullptr, 0,
								 callback, context, destroy);
	handle_write_op *op = handle_write_op_alloc(io->get_op_pools());
	if (!op)
	{
		errno = ENOMEM;
		return -1;
	}
	op->work_ = handler_work(impl_->executor_);
	op->callback_ = callback;
	op->context_ = context;
	op->destroy_ = destroy;
	EnterCriticalSection(&impl_->mutex_);
	if (impl_->handle_ == INVALID_HANDLE_VALUE)
	{
		LeaveCriticalSection(&impl_->mutex_);
	handle_write_op_free(op);
		errno = EBADF;
		return -1;
	}
	HANDLE handle = impl_->handle_;
	LeaveCriticalSection(&impl_->mutex_);
	io->get_handle_service().start_write_op(impl_, offset, buf, size, op);
	return 0;
}
int random_access_handle::async_readv_at(
	uint64_t offset, const struct iovec *iov, int iovcnt,
	void (*callback)(void *, async_error_code, size_t), void *context)
{
	return this->async_readv_at(offset, iov, iovcnt, callback, context,
								nullptr);
}
int random_access_handle::async_readv_at(uint64_t offset,
										 const struct iovec *iov, int iovcnt,
										 void (*callback)(void *, async_error_code, size_t),
										 void *context,
										 void (*destroy)(void *))
{
	if (!impl_)
	{
		errno = EINVAL;
		return -1;
	}
	if (!callback || !iov || iovcnt <= 0)
	{
		errno = EINVAL;
		return -1;
	}
	size_t total = 0;
	for (int i = 0; i < iovcnt; ++i)
	{
		if (iov[i].iov_len > ULONG_MAX ||
			total > (size_t)-1 - iov[i].iov_len)
		{
			errno = EINVAL;
			return -1;
		}
		total += iov[i].iov_len;
	}
	if (total == 0 || total > ULONG_MAX)
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
	EnterCriticalSection(&impl_->mutex_);
	bool blocking = impl_->blocking_;
	bool assigned = impl_->handle_ != INVALID_HANDLE_VALUE;
	LeaveCriticalSection(&impl_->mutex_);
	if (!assigned)
	{
		errno = EBADF;
		return -1;
	}
	if (blocking)
		return blocking_file_start(impl_, blocking_file_readv, offset, nullptr,
								  total, iov, iovcnt, callback, context, destroy);
	handle_readv_op *op = handle_readv_op_alloc(io->get_op_pools());
	if (!op)
	{
		errno = ENOMEM;
		return -1;
	}
	op->iov_ = (struct iovec *)malloc((size_t)iovcnt * sizeof(struct iovec));
	op->temp_ = (char *)malloc(total);
	if (!op->iov_ || !op->temp_)
	{
	handle_readv_op_free(op);
		errno = ENOMEM;
		return -1;
	}
	op->work_ = handler_work(impl_->executor_);
	op->callback_ = callback;
	op->context_ = context;
	op->destroy_ = destroy;
	op->iov_count_ = iovcnt;
	op->temp_size_ = total;

	for (int i = 0; i < iovcnt; ++i)
	{
		op->iov_[i] = iov[i];
	}
	EnterCriticalSection(&impl_->mutex_);
	if (impl_->handle_ == INVALID_HANDLE_VALUE)
	{
		LeaveCriticalSection(&impl_->mutex_);
	handle_readv_op_free(op);
		errno = EBADF;
		return -1;
	}
	HANDLE handle = impl_->handle_;
	LeaveCriticalSection(&impl_->mutex_);
	io->get_handle_service().start_read_op(impl_, offset, op->temp_, total, op);
	return 0;
}
int random_access_handle::async_writev_at(
	uint64_t offset, const struct iovec *iov, int iovcnt,
	void (*callback)(void *, async_error_code, size_t), void *context)
{
	return this->async_writev_at(offset, iov, iovcnt, callback, context,
								 nullptr);
}
int random_access_handle::async_writev_at(uint64_t offset,
										  const struct iovec *iov, int iovcnt,
										  void (*callback)(void *, async_error_code, size_t),
										  void *context,
										  void (*destroy)(void *))
{
	if (!impl_)
	{
		errno = EINVAL;
		return -1;
	}
	if (!callback || !iov || iovcnt <= 0)
	{
		errno = EINVAL;
		return -1;
	}
	size_t total = 0;
	for (int i = 0; i < iovcnt; ++i)
	{
		if (iov[i].iov_len > ULONG_MAX ||
			total > (size_t)-1 - iov[i].iov_len)
		{
			errno = EINVAL;
			return -1;
		}
		total += iov[i].iov_len;
	}
	if (total == 0 || total > ULONG_MAX)
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
	EnterCriticalSection(&impl_->mutex_);
	bool blocking = impl_->blocking_;
	bool assigned = impl_->handle_ != INVALID_HANDLE_VALUE;
	LeaveCriticalSection(&impl_->mutex_);
	if (!assigned)
	{
		errno = EBADF;
		return -1;
	}
	if (blocking)
		return blocking_file_start(impl_, blocking_file_writev, offset, nullptr,
								  total, iov, iovcnt, callback, context, destroy);
	handle_writev_op *op = handle_writev_op_alloc(io->get_op_pools());
	if (!op)
	{
		errno = ENOMEM;
		return -1;
	}
	op->iov_ = (struct iovec *)malloc((size_t)iovcnt * sizeof(struct iovec));
	op->temp_ = (char *)malloc(total);
	if (!op->iov_ || !op->temp_)
	{
	handle_writev_op_free(op);
		errno = ENOMEM;
		return -1;
	}
	op->work_ = handler_work(impl_->executor_);
	op->callback_ = callback;
	op->context_ = context;
	op->destroy_ = destroy;
	op->iov_count_ = iovcnt;
	op->temp_size_ = total;

	for (int i = 0; i < iovcnt; ++i)
	{
		op->iov_[i] = iov[i];
	}
	size_t copied = 0;
	for (int i = 0; i < iovcnt; ++i)
	{
		memcpy(op->temp_ + copied, iov[i].iov_base, iov[i].iov_len);
		copied += iov[i].iov_len;
	}
	EnterCriticalSection(&impl_->mutex_);
	if (impl_->handle_ == INVALID_HANDLE_VALUE)
	{
		LeaveCriticalSection(&impl_->mutex_);
	handle_writev_op_free(op);
		errno = EBADF;
		return -1;
	}
	HANDLE handle = impl_->handle_;
	LeaveCriticalSection(&impl_->mutex_);
	io->get_handle_service().start_write_op(impl_, offset, op->temp_, total, op);
	return 0;
}
int random_access_handle::async_fsync(void (*callback)(void *, async_error_code, size_t),
								  void *context,
								  void (*destroy)(void *))
{
	if (!impl_ || !callback)
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
	HANDLE duplicate = INVALID_HANDLE_VALUE;
	EnterCriticalSection(&impl_->mutex_);
	if (impl_->handle_ == INVALID_HANDLE_VALUE)
	{
		LeaveCriticalSection(&impl_->mutex_);
		errno = EBADF;
		return -1;
	}
	if (!::DuplicateHandle(::GetCurrentProcess(), impl_->handle_,
						   ::GetCurrentProcess(), &duplicate, 0, FALSE,
						   DUPLICATE_SAME_ACCESS))
	{
		int error = async_win_error_to_errno(::GetLastError());
		LeaveCriticalSection(&impl_->mutex_);
		errno = error;
		return -1;
	}
	LeaveCriticalSection(&impl_->mutex_);
	void *mem = malloc(sizeof(flush_call));
	if (!mem)
	{
		::CloseHandle(duplicate);
		errno = ENOMEM;
		return -1;
	}
	flush_call *call = new (mem) flush_call(io, impl_->executor_, duplicate,
										   callback, context, destroy);
	if (io->post_blocking(&flush_call_run, call, &flush_call_abandon) != 0)
	{
		flush_call_free(call);
		::CloseHandle(duplicate);
		return -1;
	}
	return 0;
}
int random_access_handle::cancel()
{
	if (!impl_)
		return 0;
	EnterCriticalSection(&impl_->mutex_);
	HANDLE handle = impl_->handle_;
	LeaveCriticalSection(&impl_->mutex_);
	return impl_->executor_.context()->get_handle_service().cancel(impl_);
}
int random_access_handle::release()
{
	if (!impl_)
		return 0;
	this->cancel();
	EnterCriticalSection(&impl_->mutex_);
	io_context *io = impl_->executor_.context();
	if (io)
		io->get_handle_service().unregister_handle(impl_);
	impl_->handle_ = INVALID_HANDLE_VALUE;
	LeaveCriticalSection(&impl_->mutex_);
	return 0;
}
int random_access_handle::close()
{
	if (!impl_)
		return 0;
	this->cancel();
	io_context *io = impl_->executor_.context();
	EnterCriticalSection(&impl_->mutex_);
	HANDLE handle = impl_->handle_;
	impl_->handle_ = INVALID_HANDLE_VALUE;
	LeaveCriticalSection(&impl_->mutex_);
	if (io)
		io->get_handle_service().unregister_handle(impl_);
	if (handle != INVALID_HANDLE_VALUE)
		::CloseHandle(handle);
	return 0;
}
HANDLE random_access_handle::native_handle() const
{
	return impl_ ? impl_->handle_ : INVALID_HANDLE_VALUE;
}
executor random_access_handle::get_executor() const
{
	return impl_ ? impl_->executor_ : executor();
}
bool random_access_handle::is_blocking() const
{
	if (!impl_)
		return false;
	EnterCriticalSection(const_cast<CRITICAL_SECTION *>(&impl_->mutex_));
	bool blocking = impl_->blocking_;
	LeaveCriticalSection(const_cast<CRITICAL_SECTION *>(&impl_->mutex_));
	return blocking;
}

