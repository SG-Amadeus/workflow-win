#include "tcp_socket.h"
#include "op/connect_op.h"
#include "op/recv_op.h"
#include "op/send_op.h"
#include "op/iocp_op_cancellation.h"
#include "service/socket_service.h"
#include "service/cancel_token.h"
#include "error.h"

#include <errno.h>
#include <new>
#include <stdlib.h>
#include <string.h>

#include "../list.h"

class tcp_socket::impl : public socket_impl
{
public:
	executor executor_;
	int state_;
	cancel_token *cancel_token_;
	CRITICAL_SECTION mutex_;
	volatile LONG refs_;

	explicit impl(executor ex)
		: socket_impl(),
		  executor_(ex),
		  state_(0),
		  cancel_token_(cancel_token::create())
	{
		::InitializeCriticalSection(&mutex_);
		refs_ = 1;
	}


	~impl()
	{
		if (cancel_token_)
			cancel_token_->release();
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

tcp_socket::tcp_socket()
	: impl_(nullptr)
{
}

tcp_socket *tcp_socket::create(executor ex)
{
	void *mem = malloc(sizeof(tcp_socket));
	if (!mem)
	{
		errno = ENOMEM;
		return nullptr;
	}

	tcp_socket *socket = new (mem) tcp_socket();
	if (socket->init(ex) == 0)
		return socket;

	int error = errno;
	socket->~tcp_socket();
	free(socket);
	errno = error;
	return nullptr;
}

void tcp_socket::destroy(tcp_socket *socket)
{
	if (socket)
	{
		socket->~tcp_socket();
		free(socket);
	}
}

int tcp_socket::init(executor ex)
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
	if (!impl_->cancel_token_)
	{
		impl_->~impl();
		free(impl_);
		impl_ = nullptr;
		errno = ENOMEM;
		return -1;
	}
	return 0;
}

tcp_socket::~tcp_socket()
{
	if (impl_)
	{
		this->close();
		impl_->release();
	}
}

int tcp_socket::open()
{
	return this->open(AF_INET);
}

int tcp_socket::open(int family)
{
	if (!impl_)
	{
		errno = EINVAL;
		return -1;
	}
	if (family != AF_INET && family != AF_INET6)
	{
		errno = EAFNOSUPPORT;
		return -1;
	}
	SOCKET s = ::WSASocketW(family, SOCK_STREAM, IPPROTO_TCP,
							nullptr, 0, WSA_FLAG_OVERLAPPED);
	if (s == INVALID_SOCKET)
	{
		errno = async_win_error_to_errno(::WSAGetLastError());
		return -1;
	}

	int ret = this->assign(s);
	if (ret != 0)
		::closesocket(s);
	return ret;
}

int tcp_socket::assign(SOCKET socket)
{
	if (!impl_)
	{
		errno = EINVAL;
		return -1;
	}
	if (socket == INVALID_SOCKET)
	{
		errno = EINVAL;
		return -1;
	}

	u_long nonblocking = 1;
	if (::ioctlsocket(socket, FIONBIO, &nonblocking) != 0)
	{
		errno = async_win_error_to_errno(::WSAGetLastError());
		return -1;
	}

	io_context *io = impl_->executor_.context();
	if (!io)
	{
		errno = EINVAL;
		return -1;
	}

	EnterCriticalSection(&impl_->mutex_);
	if (impl_->socket_ != INVALID_SOCKET)
	{
		LeaveCriticalSection(&impl_->mutex_);
		errno = EBUSY;
		return -1;
	}
	impl_->state_ = 1; /* stream-oriented */
	impl_->cancel_token_->reset();
	LeaveCriticalSection(&impl_->mutex_);

	if (io->get_socket_service().register_socket(impl_, socket) != 0)
	{
		EnterCriticalSection(&impl_->mutex_);
		impl_->state_ = 0;
		LeaveCriticalSection(&impl_->mutex_);
		return -1;
	}

	return 0;
}

int tcp_socket::close()
{
	if (!impl_)
		return 0;
	impl_->cancel_token_->close();
	this->cancel();

	io_context *io = impl_->executor_.context();
	EnterCriticalSection(&impl_->mutex_);
	SOCKET s = impl_->socket_;
	impl_->socket_ = INVALID_SOCKET;
	impl_->state_ = 0;
	LeaveCriticalSection(&impl_->mutex_);

	if (io)
		io->get_socket_service().unregister_socket(impl_);

	if (s != INVALID_SOCKET)
		::closesocket(s);

	return 0;
}

int tcp_socket::async_connect(const struct sockaddr *addr, int addrlen,
								  void (*callback)(void *, async_error_code), void *context)
{
	return this->async_connect(addr, addrlen, callback, context, nullptr);
}

int tcp_socket::async_connect(const struct sockaddr *addr, int addrlen,
								  void (*callback)(void *, async_error_code), void *context,
								  void (*destroy)(void *))
{
	return this->async_connect(addr, addrlen, callback, context,
							   cancellation_slot(), destroy);
}

int tcp_socket::async_connect(const struct sockaddr *addr, int addrlen,
								  void (*callback)(void *, async_error_code), void *context,
								  const cancellation_slot &slot,
								  void (*destroy)(void *))
{
	if (!impl_)
	{
		errno = EINVAL;
		return -1;
	}
	if (!callback || !addr || addrlen <= 0)
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
	if (impl_->socket_ == INVALID_SOCKET)
	{
		LeaveCriticalSection(&impl_->mutex_);
		errno = EBADF;
		return -1;
	}
	SOCKET s = impl_->socket_;
	LeaveCriticalSection(&impl_->mutex_);

	connect_op *op = (connect_op *)connect_op_alloc(io->get_op_pools());
	if (!op)
	{
		errno = ENOMEM;
		return -1;
	}
	op->work_ = handler_work(impl_->executor_);
	op->socket_ = s;
	op->cancel_token_ = impl_->cancel_token_;
	op->cancel_token_->acquire();
	op->callback_ = callback;
	op->context_ = context;
	op->destroy_ = destroy;

	EnterCriticalSection(&impl_->mutex_);
	if (impl_->socket_ != s)
	{
		LeaveCriticalSection(&impl_->mutex_);
		connect_op_free(op);
		errno = EBUSY;
		return -1;
	}
	LeaveCriticalSection(&impl_->mutex_);

	win_iocp_operation *iocp_op = op;
	iocp_op_cancellation *cancellation = nullptr;
	if (slot.is_connected())
	{
		cancellation = iocp_op_cancellation::create((HANDLE)s, op, slot);
		if (!cancellation)
		{
			connect_op_free(op);
			return -1;
		}
		iocp_op = cancellation;
	}

	io->get_socket_service().start_connect_op(
		impl_, addr, addrlen, iocp_op);
	return 0;
}

int tcp_socket::async_read_some(void *buf, size_t size,
								void (*callback)(void *, async_error_code, size_t),
								void *context)
{
	return this->async_read_some(buf, size, callback, context, nullptr);
}

int tcp_socket::async_read_some(void *buf, size_t size,
								void (*callback)(void *, async_error_code, size_t),
								void *context, void (*destroy)(void *))
{
	return this->async_read_some(buf, size, callback, context,
								 cancellation_slot(), destroy);
}

int tcp_socket::async_read_some(void *buf, size_t size,
								void (*callback)(void *, async_error_code, size_t), void *context,
								const cancellation_slot &slot,
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

	recv_op *op = recv_op_alloc(io->get_op_pools());
	if (!op)
	{
		errno = ENOMEM;
		return -1;
	}
	op->work_ = handler_work(impl_->executor_);
	op->cancel_token_ = impl_->cancel_token_;
	op->cancel_token_->acquire();
	op->callback_ = callback;
	op->context_ = context;
	op->destroy_ = destroy;

	EnterCriticalSection(&impl_->mutex_);
	if (impl_->socket_ == INVALID_SOCKET)
	{
		LeaveCriticalSection(&impl_->mutex_);
		recv_op_free(op);
		errno = EBADF;
		return -1;
	}
	SOCKET s = impl_->socket_;
	LeaveCriticalSection(&impl_->mutex_);

	win_iocp_operation *iocp_op = op;
	if (slot.is_connected())
	{
		iocp_op_cancellation *cancellation =
			iocp_op_cancellation::create((HANDLE)s, op, slot);
		if (!cancellation)
		{
			recv_op_free(op);
			return -1;
		}
		iocp_op = cancellation;
	}

	WSABUF wsa_buf;
	wsa_buf.buf = static_cast<char *>(buf);
	wsa_buf.len = (ULONG)size;
	io->get_socket_service().start_receive_op(impl_, &wsa_buf, 1, 0,
											 false, iocp_op);
	return 0;
}

int tcp_socket::async_readv_some(const struct iovec *iov, int iovcnt,
								 void (*callback)(void *, async_error_code, size_t),
								 void *context)
{
	return this->async_readv_some(iov, iovcnt, callback, context, nullptr);
}

int tcp_socket::async_readv_some(const struct iovec *iov, int iovcnt,
								 void (*callback)(void *, async_error_code, size_t),
								 void *context, void (*destroy)(void *))
{
	return this->async_readv_some(iov, iovcnt, callback, context,
								  cancellation_slot(), destroy);
}

int tcp_socket::async_readv_some(const struct iovec *iov, int iovcnt,
								 void (*callback)(void *, async_error_code, size_t), void *context,
								 const cancellation_slot &slot,
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
	if ((size_t)iovcnt > (size_t)-1 / sizeof(WSABUF))
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

	recv_op *op = recv_op_alloc(io->get_op_pools());
	if (!op)
	{
		errno = ENOMEM;
		return -1;
	}
	op->work_ = handler_work(impl_->executor_);
	op->cancel_token_ = impl_->cancel_token_;
	op->cancel_token_->acquire();
	op->callback_ = callback;
	op->context_ = context;
	op->destroy_ = destroy;

	WSABUF *wsa_bufs = (WSABUF *)malloc(sizeof(WSABUF) * (size_t)iovcnt);
	if (!wsa_bufs)
	{
		recv_op_free( op);
		errno = ENOMEM;
		return -1;
	}

	for (int i = 0; i < iovcnt; ++i)
	{
		if (iov[i].iov_len > ULONG_MAX)
		{
			free(wsa_bufs);
			recv_op_free( op);
			errno = EINVAL;
			return -1;
		}
		wsa_bufs[i].buf = static_cast<char *>(iov[i].iov_base);
		wsa_bufs[i].len = (ULONG)iov[i].iov_len;
	}

	EnterCriticalSection(&impl_->mutex_);
	if (impl_->socket_ == INVALID_SOCKET)
	{
		LeaveCriticalSection(&impl_->mutex_);
		recv_op_free(op);
		errno = EBADF;
		return -1;
	}
	SOCKET s = impl_->socket_;
	LeaveCriticalSection(&impl_->mutex_);

	win_iocp_operation *iocp_op = op;
	if (slot.is_connected())
	{
		iocp_op_cancellation *cancellation =
			iocp_op_cancellation::create((HANDLE)s, op, slot);
		if (!cancellation)
		{
			free(wsa_bufs);
			recv_op_free(op);
			return -1;
		}
		iocp_op = cancellation;
	}

	io->get_socket_service().start_receive_op(
		impl_, wsa_bufs, (DWORD)iovcnt, 0, false, iocp_op);
	free(wsa_bufs);
	return 0;
}

int tcp_socket::async_wait_read(void (*callback)(void *, async_error_code, size_t),
								void *context, void (*destroy)(void *))
{
	return this->async_wait_read(callback, context, cancellation_slot(),
								 destroy);
}

int tcp_socket::async_wait_read(void (*callback)(void *, async_error_code, size_t),
								void *context, const cancellation_slot &slot,
								void (*destroy)(void *))
{
	if (!impl_)
	{
		errno = EINVAL;
		return -1;
	}
	if (!callback)
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

	recv_op *op = recv_op_alloc(io->get_op_pools());
	if (!op)
	{
		errno = ENOMEM;
		return -1;
	}
	op->work_ = handler_work(impl_->executor_);
	op->cancel_token_ = impl_->cancel_token_;
	op->cancel_token_->acquire();
	op->callback_ = callback;
	op->context_ = context;
	op->destroy_ = destroy;
	op->all_empty_ = true;

	EnterCriticalSection(&impl_->mutex_);
	if (impl_->socket_ == INVALID_SOCKET)
	{
		LeaveCriticalSection(&impl_->mutex_);
		recv_op_free(op);
		errno = EBADF;
		return -1;
	}
	SOCKET s = impl_->socket_;
	LeaveCriticalSection(&impl_->mutex_);

	win_iocp_operation *iocp_op = op;
	if (slot.is_connected())
	{
		iocp_op_cancellation *cancellation =
			iocp_op_cancellation::create((HANDLE)s, op, slot);
		if (!cancellation)
		{
			recv_op_free(op);
			return -1;
		}
		iocp_op = cancellation;
	}

	WSABUF wsa_buf = {0, nullptr};
	io->get_socket_service().start_receive_op(impl_, &wsa_buf, 1, 0,
											 false, iocp_op);
	return 0;
}

int tcp_socket::async_write_some(const void *buf, size_t size,
								 void (*callback)(void *, async_error_code, size_t),
								 void *context)
{
	return this->async_write_some(buf, size, callback, context, nullptr);
}

int tcp_socket::async_write_some(const void *buf, size_t size,
								 void (*callback)(void *, async_error_code, size_t),
								 void *context, void (*destroy)(void *))
{
	return this->async_write_some(buf, size, callback, context,
								  cancellation_slot(), destroy);
}

int tcp_socket::async_write_some(const void *buf, size_t size,
								 void (*callback)(void *, async_error_code, size_t), void *context,
								 const cancellation_slot &slot,
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

	send_op *op = send_op_alloc(io->get_op_pools());
	if (!op)
	{
		errno = ENOMEM;
		return -1;
	}
	op->work_ = handler_work(impl_->executor_);
	op->cancel_token_ = impl_->cancel_token_;
	op->cancel_token_->acquire();
	op->callback_ = callback;
	op->context_ = context;
	op->destroy_ = destroy;

	EnterCriticalSection(&impl_->mutex_);
	if (impl_->socket_ == INVALID_SOCKET)
	{
		LeaveCriticalSection(&impl_->mutex_);
		send_op_free(op);
		errno = EBADF;
		return -1;
	}
	SOCKET s = impl_->socket_;
	LeaveCriticalSection(&impl_->mutex_);

	win_iocp_operation *iocp_op = op;
	if (slot.is_connected())
	{
		iocp_op_cancellation *cancellation =
			iocp_op_cancellation::create((HANDLE)s, op, slot);
		if (!cancellation)
		{
			send_op_free(op);
			return -1;
		}
		iocp_op = cancellation;
	}

	WSABUF wsa_buf;
	wsa_buf.buf = const_cast<char *>(static_cast<const char *>(buf));
	wsa_buf.len = (ULONG)size;
	io->get_socket_service().start_send_op(impl_, &wsa_buf, 1, 0,
										  false, iocp_op);
	return 0;
}

int tcp_socket::async_writev_some(const struct iovec *iov, int iovcnt,
								  void (*callback)(void *, async_error_code, size_t),
								  void *context)
{
	return this->async_writev_some(iov, iovcnt, callback, context, nullptr);
}

int tcp_socket::async_writev_some(const struct iovec *iov, int iovcnt,
								  void (*callback)(void *, async_error_code, size_t),
								  void *context, void (*destroy)(void *))
{
	return this->async_writev_some(iov, iovcnt, callback, context,
								   cancellation_slot(), destroy);
}

int tcp_socket::async_writev_some(const struct iovec *iov, int iovcnt,
								  void (*callback)(void *, async_error_code, size_t), void *context,
								  const cancellation_slot &slot,
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
	if ((size_t)iovcnt > (size_t)-1 / sizeof(WSABUF))
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

	send_op *op = send_op_alloc(io->get_op_pools());
	if (!op)
	{
		errno = ENOMEM;
		return -1;
	}
	op->work_ = handler_work(impl_->executor_);
	op->cancel_token_ = impl_->cancel_token_;
	op->cancel_token_->acquire();
	op->callback_ = callback;
	op->context_ = context;
	op->destroy_ = destroy;

	WSABUF *wsa_bufs = (WSABUF *)malloc(sizeof(WSABUF) * (size_t)iovcnt);
	if (!wsa_bufs)
	{
		send_op_free( op);
		errno = ENOMEM;
		return -1;
	}

	for (int i = 0; i < iovcnt; ++i)
	{
		if (iov[i].iov_len > ULONG_MAX)
		{
			free(wsa_bufs);
			send_op_free( op);
			errno = EINVAL;
			return -1;
		}
		wsa_bufs[i].buf = const_cast<char *>(
			static_cast<const char *>(iov[i].iov_base));
		wsa_bufs[i].len = (ULONG)iov[i].iov_len;
	}

	EnterCriticalSection(&impl_->mutex_);
	if (impl_->socket_ == INVALID_SOCKET)
	{
		LeaveCriticalSection(&impl_->mutex_);
		send_op_free(op);
		errno = EBADF;
		return -1;
	}
	SOCKET s = impl_->socket_;
	LeaveCriticalSection(&impl_->mutex_);

	win_iocp_operation *iocp_op = op;
	if (slot.is_connected())
	{
		iocp_op_cancellation *cancellation =
			iocp_op_cancellation::create((HANDLE)s, op, slot);
		if (!cancellation)
		{
			free(wsa_bufs);
			send_op_free(op);
			return -1;
		}
		iocp_op = cancellation;
	}

	io->get_socket_service().start_send_op(
		impl_, wsa_bufs, (DWORD)iovcnt, 0, false, iocp_op);
	free(wsa_bufs);
	return 0;
}

int tcp_socket::write_some(const void *buf, size_t size)
{
	if (!impl_)
	{
		errno = EINVAL;
		return -1;
	}
	if (!buf && size != 0)
	{
		errno = EINVAL;
		return -1;
	}
	if (size > (size_t)INT_MAX)
	{
		errno = EOVERFLOW;
		return -1;
	}
	if (size == 0)
		return 0;

	EnterCriticalSection(&impl_->mutex_);
	SOCKET socket = impl_->socket_;
	if (socket == INVALID_SOCKET)
	{
		LeaveCriticalSection(&impl_->mutex_);
		errno = EBADF;
		return -1;
	}
	int ret = ::send(socket, static_cast<const char *>(buf),
		static_cast<int>(size), 0);
	LeaveCriticalSection(&impl_->mutex_);
	if (ret == SOCKET_ERROR)
	{
		errno = async_win_error_to_errno(::WSAGetLastError());
		return -1;
	}
	return ret;
}

int tcp_socket::cancel()
{
	if (!impl_)
		return 0;
	EnterCriticalSection(&impl_->mutex_);
	SOCKET s = impl_->socket_;
	LeaveCriticalSection(&impl_->mutex_);

	return impl_->executor_.context()->get_socket_service().cancel(impl_);
}

int tcp_socket::cancel_connect()
{
	return this->cancel();
}

int tcp_socket::cancel_read()
{
	return this->cancel();
}

int tcp_socket::cancel_write()
{
	return this->cancel();
}

SOCKET tcp_socket::native_handle() const
{
	return impl_ ? impl_->socket_ : INVALID_SOCKET;
}

executor tcp_socket::get_executor() const
{
	return impl_ ? impl_->executor_ : executor();
}

