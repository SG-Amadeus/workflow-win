#include "handle_service.h"
#include "../io_context.h"
#include "../error.h"

#include <errno.h>

handle_service::handle_service(io_context *io)
	: io_(io)
{
	::InitializeCriticalSection(&lock_);
	INIT_LIST_HEAD(&handles_);
}

handle_service::~handle_service()
{
	this->shutdown();
	::DeleteCriticalSection(&lock_);
}

int handle_service::register_handle(handle_impl *impl, HANDLE handle,
									 bool associate_iocp)
{
	if (!impl || !handle || handle == INVALID_HANDLE_VALUE)
	{
		errno = EINVAL;
		return -1;
	}

	::EnterCriticalSection(&lock_);
	if (!list_empty(&impl->registry_node_))
	{
		::LeaveCriticalSection(&lock_);
		errno = EBUSY;
		return -1;
	}

	if (associate_iocp && (!io_ || io_->register_handle(handle) != 0))
	{
		errno = io_ ? errno : EINVAL;
		::LeaveCriticalSection(&lock_);
		return -1;
	}

	impl->handle_ = handle;
	impl->safe_cancellation_thread_id_ = 0;
	list_add_tail(&impl->registry_node_, &handles_);
	::LeaveCriticalSection(&lock_);

	return 0;
}

void handle_service::unregister_handle(handle_impl *impl)
{
	if (!impl)
		return;

	::EnterCriticalSection(&lock_);
	if (!list_empty(&impl->registry_node_))
	{
		list_del(&impl->registry_node_);
		INIT_LIST_HEAD(&impl->registry_node_);
	}
	impl->handle_ = INVALID_HANDLE_VALUE;
	impl->safe_cancellation_thread_id_ = 0;
	::LeaveCriticalSection(&lock_);
}

int handle_service::cancel(handle_impl *impl)
{
	if (!impl || impl->handle_ == INVALID_HANDLE_VALUE)
	{
		errno = EBADF;
		return -1;
	}

	HANDLE handle = impl->handle_;
	FARPROC cancel_io_ex_ptr = ::GetProcAddress(
		::GetModuleHandleA("KERNEL32"), "CancelIoEx");
	if (cancel_io_ex_ptr)
	{
		typedef BOOL (WINAPI *cancel_io_ex_type)(HANDLE, LPOVERLAPPED);
		cancel_io_ex_type cancel_io_ex =
			reinterpret_cast<cancel_io_ex_type>(cancel_io_ex_ptr);
		if (cancel_io_ex(handle, nullptr))
			return 0;

		DWORD error = ::GetLastError();
		if (error == ERROR_NOT_FOUND)
			return 0;
		errno = async_win_error_to_errno((int)error);
		return -1;
	}

	if (impl->safe_cancellation_thread_id_ == 0)
		return 0;
	if (impl->safe_cancellation_thread_id_ != ::GetCurrentThreadId())
	{
		errno = ERROR_NOT_SUPPORTED;
		return -1;
	}

	if (::CancelIo(handle))
		return 0;

	DWORD error = ::GetLastError();
	if (error == ERROR_NOT_FOUND)
		return 0;

	errno = async_win_error_to_errno((int)error);
	return -1;
}

void handle_service::update_cancellation_thread_id(handle_impl *impl)
{
	if (impl->safe_cancellation_thread_id_ == 0)
		impl->safe_cancellation_thread_id_ = ::GetCurrentThreadId();
	else if (impl->safe_cancellation_thread_id_ != ::GetCurrentThreadId())
		impl->safe_cancellation_thread_id_ = ~DWORD(0);
}

void handle_service::start_read_op(handle_impl *impl, uint64_t offset,
								void *buffer, size_t size,
								win_iocp_operation *op)
{
	HANDLE handle = impl ? impl->handle_ : INVALID_HANDLE_VALUE;
	if (impl)
		this->update_cancellation_thread_id(impl);
	io_->work_started();

	if (!impl || !handle || handle == INVALID_HANDLE_VALUE)
	{
		io_->on_completion(op, async_system_error(ERROR_INVALID_HANDLE), 0);
		return;
	}

	if (size == 0)
	{
		io_->on_completion(op, async_error_code(), 0);
		return;
	}

	op->Offset = (DWORD)offset;
	op->OffsetHigh = (DWORD)(offset >> 32);
	DWORD bytes = 0;
	BOOL ok = ::ReadFile(handle, buffer, (DWORD)size, &bytes, op);
	DWORD error = ::GetLastError();
	if (!ok && error != ERROR_IO_PENDING && error != ERROR_MORE_DATA)
		io_->on_completion(op, async_native_error(error), bytes);
	else
		io_->on_pending(op);
}

void handle_service::start_write_op(handle_impl *impl, uint64_t offset,
								 const void *buffer, size_t size,
								 win_iocp_operation *op)
{
	HANDLE handle = impl ? impl->handle_ : INVALID_HANDLE_VALUE;
	if (impl)
		this->update_cancellation_thread_id(impl);
	io_->work_started();

	if (!impl || !handle || handle == INVALID_HANDLE_VALUE)
	{
		io_->on_completion(op, async_system_error(ERROR_INVALID_HANDLE), 0);
		return;
	}

	if (size == 0)
	{
		io_->on_completion(op, async_error_code(), 0);
		return;
	}

	op->Offset = (DWORD)offset;
	op->OffsetHigh = (DWORD)(offset >> 32);
	DWORD bytes = 0;
	BOOL ok = ::WriteFile(handle, buffer, (DWORD)size, &bytes, op);
	DWORD error = ::GetLastError();
	if (!ok && error != ERROR_IO_PENDING && error != ERROR_MORE_DATA)
		io_->on_completion(op, async_native_error(error), bytes);
	else
		io_->on_pending(op);
}

void handle_service::shutdown()
{
	struct list_head *pos;
	struct list_head *n;

	::EnterCriticalSection(&lock_);
	list_for_each_safe(pos, n, &handles_)
	{
		handle_impl *impl = list_entry(pos, handle_impl, registry_node_);
		list_del(&impl->registry_node_);
		INIT_LIST_HEAD(&impl->registry_node_);
		if (impl->handle_ != INVALID_HANDLE_VALUE)
			::CloseHandle(impl->handle_);
		impl->handle_ = INVALID_HANDLE_VALUE;
		impl->safe_cancellation_thread_id_ = 0;
	}
	::LeaveCriticalSection(&lock_);
}

bool handle_service::is_registered(handle_impl *impl) const
{
	if (!impl)
		return false;

	bool registered;
	::EnterCriticalSection(const_cast<CRITICAL_SECTION *>(&lock_));
	registered = !list_empty(&impl->registry_node_);
	::LeaveCriticalSection(const_cast<CRITICAL_SECTION *>(&lock_));
	return registered;
}

