#include "socket_service.h"
#include "../io_context.h"
#include "../error.h"

#include <errno.h>

socket_service::socket_service(io_context *io)
	: io_(io)
{
	::InitializeCriticalSection(&lock_);
	INIT_LIST_HEAD(&sockets_);
}

socket_service::~socket_service()
{
	this->shutdown();
	::DeleteCriticalSection(&lock_);
}

int socket_service::register_socket(socket_impl *impl, SOCKET socket)
{
	if (!impl || socket == INVALID_SOCKET)
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
	if (!io_ || io_->register_handle((HANDLE)socket) != 0)
	{
		errno = io_ ? errno : EINVAL;
		::LeaveCriticalSection(&lock_);
		return -1;
	}
	impl->socket_ = socket;
	impl->safe_cancellation_thread_id_ = 0;
	impl->connect_ex_ = nullptr;
	impl->connect_ex_unavailable_ = false;
	list_add_tail(&impl->registry_node_, &sockets_);
	::LeaveCriticalSection(&lock_);

	return 0;
}

void socket_service::unregister_socket(socket_impl *impl)
{
	if (!impl)
		return;

	::EnterCriticalSection(&lock_);
	if (!list_empty(&impl->registry_node_))
	{
		list_del(&impl->registry_node_);
		INIT_LIST_HEAD(&impl->registry_node_);
	}
	impl->socket_ = INVALID_SOCKET;
	impl->safe_cancellation_thread_id_ = 0;
	impl->connect_ex_ = nullptr;
	impl->connect_ex_unavailable_ = false;
	::LeaveCriticalSection(&lock_);
}

int socket_service::cancel(socket_impl *impl)
{
	if (!impl || impl->socket_ == INVALID_SOCKET)
	{
		errno = EBADF;
		return -1;
	}

	SOCKET socket = impl->socket_;
	FARPROC cancel_io_ex_ptr = ::GetProcAddress(
		::GetModuleHandleA("KERNEL32"), "CancelIoEx");
	if (cancel_io_ex_ptr)
	{
		typedef BOOL (WINAPI *cancel_io_ex_type)(HANDLE, LPOVERLAPPED);
		cancel_io_ex_type cancel_io_ex =
			reinterpret_cast<cancel_io_ex_type>(cancel_io_ex_ptr);
		if (cancel_io_ex((HANDLE)socket, nullptr))
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

	if (::CancelIo((HANDLE)socket))
		return 0;

	DWORD error = ::GetLastError();
	if (error == ERROR_NOT_FOUND)
		return 0;

	errno = async_win_error_to_errno((int)error);
	return -1;
}

void socket_service::update_cancellation_thread_id(socket_impl *impl)
{
	if (impl->safe_cancellation_thread_id_ == 0)
		impl->safe_cancellation_thread_id_ = ::GetCurrentThreadId();
	else if (impl->safe_cancellation_thread_id_ != ::GetCurrentThreadId())
		impl->safe_cancellation_thread_id_ = ~DWORD(0);
}

void socket_service::start_connect_op(socket_impl *impl,
								   const struct sockaddr *addr, int addrlen,
								   win_iocp_operation *op)
{
	SOCKET socket = impl ? impl->socket_ : INVALID_SOCKET;
	if (impl)
		this->update_cancellation_thread_id(impl);
	io_->work_started();
	if (!impl || socket == INVALID_SOCKET || !addr || addrlen <= 0)
	{
		io_->on_completion(op, !impl || socket == INVALID_SOCKET
			? async_socket_error(WSAENOTSOCK)
			: async_socket_error(WSAEINVAL), 0);
		return;
	}

	if (!impl->connect_ex_ && !impl->connect_ex_unavailable_)
	{
		GUID guid = WSAID_CONNECTEX;
		DWORD bytes = 0;
		LPFN_CONNECTEX fn = nullptr;
		if (::WSAIoctl(socket, SIO_GET_EXTENSION_FUNCTION_POINTER,
						&guid, sizeof(guid), &fn, sizeof(fn), &bytes,
						nullptr, nullptr) == 0)
			impl->connect_ex_ = fn;
		else
			impl->connect_ex_unavailable_ = true;
	}

	if (!impl->connect_ex_)
	{
		io_->on_completion(op, async_socket_error(WSAEOPNOTSUPP), 0);
		return;
	}

	/* ConnectEx requires a local bind.  ASIO performs this in the socket
	 * service, keeping the public socket free of a second connect protocol. */
	int bind_result = SOCKET_ERROR;
	if (addr->sa_family == AF_INET)
	{
		SOCKADDR_IN local;
		::ZeroMemory(&local, sizeof(local));
		local.sin_family = AF_INET;
		bind_result = ::bind(socket, reinterpret_cast<sockaddr *>(&local),
						 sizeof(local));
	}
	else if (addr->sa_family == AF_INET6)
	{
		SOCKADDR_IN6 local;
		::ZeroMemory(&local, sizeof(local));
		local.sin6_family = AF_INET6;
		bind_result = ::bind(socket, reinterpret_cast<sockaddr *>(&local),
						 sizeof(local));
	}
	if (addr->sa_family != AF_INET && addr->sa_family != AF_INET6)
	{
		io_->on_completion(op, async_socket_error(WSAEAFNOSUPPORT), 0);
		return;
	}
	if (bind_result != 0)
	{
		DWORD error = ::WSAGetLastError();
		if (error != WSAEINVAL)
		{
			io_->on_completion(op, async_socket_error(static_cast<int>(error)), 0);
			return;
		}
	}

	BOOL ok = impl->connect_ex_(socket, addr, addrlen,
							 nullptr, 0, nullptr, op);
	async_error_code error = async_native_error(::WSAGetLastError());
	if (!ok && error.value() != WSA_IO_PENDING)
		io_->on_completion(op, error, 0);
	else
		io_->on_pending(op);
}

void socket_service::start_accept_op(socket_impl *impl, int family, int type,
								  int protocol, SOCKET *accepted, void *buffer,
								  DWORD address_length,
								  win_iocp_operation *op)
{
	SOCKET listener = impl ? impl->socket_ : INVALID_SOCKET;
	if (impl)
		this->update_cancellation_thread_id(impl);
	io_->work_started();
	if (!impl || listener == INVALID_SOCKET || !accepted)
	{
		io_->on_completion(op, async_socket_error(WSAENOTSOCK), 0);
		return;
	}

	*accepted = ::WSASocketW(family, type, protocol, nullptr, 0,
							WSA_FLAG_OVERLAPPED);
	if (*accepted == INVALID_SOCKET)
	{
		io_->on_completion(op, async_socket_error(::WSAGetLastError()), 0);
		return;
	}

	DWORD bytes = 0;
	BOOL ok = ::AcceptEx(listener, *accepted, buffer, 0,
						 address_length, address_length, &bytes, op);
	async_error_code error = async_native_error(::WSAGetLastError());
	if (!ok && error.value() != WSA_IO_PENDING)
		io_->on_completion(op, error, bytes);
	else
		io_->on_pending(op);
}

void socket_service::start_receive_op(socket_impl *impl, WSABUF *buffers,
								   DWORD count, DWORD flags, bool noop,
								   win_iocp_operation *op)
{
	SOCKET socket = impl ? impl->socket_ : INVALID_SOCKET;
	if (impl)
		this->update_cancellation_thread_id(impl);
	io_->work_started();
	if (noop)
	{
		io_->on_completion(op, async_error_code(), 0);
		return;
	}
	if (!impl || socket == INVALID_SOCKET)
	{
		io_->on_completion(op, async_socket_error(WSAENOTSOCK), 0);
		return;
	}

	DWORD bytes = 0;
	int rc = ::WSARecv(socket, buffers, count, &bytes, &flags, op, nullptr);
	async_error_code error = async_native_error(::WSAGetLastError());
	if (rc != 0 && error.value() != WSA_IO_PENDING)
		io_->on_completion(op, error, bytes);
	else
		io_->on_pending(op);
}

void socket_service::start_send_op(socket_impl *impl, WSABUF *buffers,
								DWORD count, DWORD flags, bool noop,
								win_iocp_operation *op)
{
	SOCKET socket = impl ? impl->socket_ : INVALID_SOCKET;
	if (impl)
		this->update_cancellation_thread_id(impl);
	io_->work_started();
	if (noop)
	{
		io_->on_completion(op, async_error_code(), 0);
		return;
	}
	if (!impl || socket == INVALID_SOCKET)
	{
		io_->on_completion(op, async_socket_error(WSAENOTSOCK), 0);
		return;
	}

	DWORD bytes = 0;
	int rc = ::WSASend(socket, buffers, count, &bytes, flags, op, nullptr);
	async_error_code error = async_native_error(::WSAGetLastError());
	if (rc != 0 && error.value() != WSA_IO_PENDING)
		io_->on_completion(op, error, bytes);
	else
		io_->on_pending(op);
}

void socket_service::start_receive_from_op(socket_impl *impl, WSABUF *buffers,
									DWORD count, DWORD flags,
									struct sockaddr *addr, int *addrlen,
									win_iocp_operation *op)
{
	SOCKET socket = impl ? impl->socket_ : INVALID_SOCKET;
	if (impl)
		this->update_cancellation_thread_id(impl);
	io_->work_started();
	if (!impl || socket == INVALID_SOCKET)
	{
		io_->on_completion(op, async_socket_error(WSAENOTSOCK), 0);
		return;
	}

	DWORD bytes = 0;
	int rc = ::WSARecvFrom(socket, buffers, count, &bytes, &flags,
						   addr, addrlen, op, nullptr);
	async_error_code error = async_native_error(::WSAGetLastError());
	if (rc != 0 && error.value() != WSA_IO_PENDING)
		io_->on_completion(op, error, bytes);
	else
		io_->on_pending(op);
}

void socket_service::start_send_to_op(socket_impl *impl, WSABUF *buffers,
								  DWORD count, DWORD flags,
								  const struct sockaddr *addr, int addrlen,
								  win_iocp_operation *op)
{
	SOCKET socket = impl ? impl->socket_ : INVALID_SOCKET;
	if (impl)
		this->update_cancellation_thread_id(impl);
	io_->work_started();
	if (!impl || socket == INVALID_SOCKET)
	{
		io_->on_completion(op, async_socket_error(WSAENOTSOCK), 0);
		return;
	}

	DWORD bytes = 0;
	int rc = ::WSASendTo(socket, buffers, count, &bytes, flags,
						  addr, addrlen, op, nullptr);
	async_error_code error = async_native_error(::WSAGetLastError());
	if (rc != 0 && error.value() != WSA_IO_PENDING)
		io_->on_completion(op, error, bytes);
	else
		io_->on_pending(op);
}

void socket_service::shutdown()
{
	struct list_head *pos;
	struct list_head *n;

	::EnterCriticalSection(&lock_);
	list_for_each_safe(pos, n, &sockets_)
	{
		socket_impl *impl = list_entry(pos, socket_impl, registry_node_);
		list_del(&impl->registry_node_);
		INIT_LIST_HEAD(&impl->registry_node_);
		if (impl->socket_ != INVALID_SOCKET)
			::closesocket(impl->socket_);
		impl->socket_ = INVALID_SOCKET;
		impl->connect_ex_ = nullptr;
		impl->connect_ex_unavailable_ = false;
		impl->safe_cancellation_thread_id_ = 0;
	}
	::LeaveCriticalSection(&lock_);
}

bool socket_service::is_registered(socket_impl *impl) const
{
	if (!impl)
		return false;

	bool registered;
	::EnterCriticalSection(const_cast<CRITICAL_SECTION *>(&lock_));
	registered = !list_empty(&impl->registry_node_);
	::LeaveCriticalSection(const_cast<CRITICAL_SECTION *>(&lock_));
	return registered;
}

