#include "tcp_acceptor.h"
#include "op/accept_op.h"
#include "op/iocp_op_cancellation.h"
#include "service/socket_service.h"
#include "service/cancel_token.h"
#include "error.h"

#include <errno.h>
#include <new>
#include <stdlib.h>

#include "../list.h"
class tcp_acceptor::impl : public socket_impl
{
public:
	executor executor_;
	cancel_token *cancel_token_;
	CRITICAL_SECTION mutex_;
	volatile LONG refs_;

	explicit impl(executor ex)
		: socket_impl(),
		  executor_(ex),
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

static int listener_family(SOCKET s)
{
	struct sockaddr_storage addr;
	ZeroMemory(&addr, sizeof addr);
	int len = sizeof addr;
	if (getsockname(s, (struct sockaddr *)&addr, &len) == 0)
		return addr.ss_family;
	return AF_INET;
}

tcp_acceptor::tcp_acceptor()
	: impl_(nullptr)
{
}

tcp_acceptor *tcp_acceptor::create(executor ex)
{
	void *mem = malloc(sizeof(tcp_acceptor));
	if (!mem)
	{
		errno = ENOMEM;
		return nullptr;
	}

	tcp_acceptor *acceptor = new (mem) tcp_acceptor();
	if (acceptor->init(ex) == 0)
		return acceptor;

	int error = errno;
	acceptor->~tcp_acceptor();
	free(acceptor);
	errno = error;
	return nullptr;
}

void tcp_acceptor::destroy(tcp_acceptor *acceptor)
{
	if (acceptor)
	{
		acceptor->~tcp_acceptor();
		free(acceptor);
	}
}

int tcp_acceptor::init(executor ex)
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

tcp_acceptor::~tcp_acceptor()
{
	if (impl_)
	{
		this->close();
		impl_->release();
	}
}

int tcp_acceptor::open()
{
	return this->open(AF_INET);
}

int tcp_acceptor::open(int family)
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

int tcp_acceptor::assign(SOCKET listener)
{
	if (!impl_)
	{
		errno = EINVAL;
		return -1;
	}
	if (listener == INVALID_SOCKET)
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
	if (impl_->socket_ != INVALID_SOCKET)
	{
		LeaveCriticalSection(&impl_->mutex_);
		errno = EBUSY;
		return -1;
	}
	impl_->cancel_token_->reset();
	LeaveCriticalSection(&impl_->mutex_);

	if (io->get_socket_service().register_socket(impl_, listener) != 0)
		return -1;

	return 0;
}

int tcp_acceptor::bind(const struct sockaddr *addr, int addrlen)
{
	if (!impl_)
	{
		errno = EINVAL;
		return -1;
	}
	EnterCriticalSection(&impl_->mutex_);
	SOCKET s = impl_->socket_;
	LeaveCriticalSection(&impl_->mutex_);

	if (s == INVALID_SOCKET)
	{
		errno = EBADF;
		return -1;
	}

	if (::bind(s, addr, addrlen) != 0)
	{
		errno = async_win_error_to_errno(::WSAGetLastError());
		return -1;
	}

	return 0;
}

int tcp_acceptor::listen(int backlog)
{
	if (!impl_)
	{
		errno = EINVAL;
		return -1;
	}
	EnterCriticalSection(&impl_->mutex_);
	SOCKET s = impl_->socket_;
	LeaveCriticalSection(&impl_->mutex_);

	if (s == INVALID_SOCKET)
	{
		errno = EBADF;
		return -1;
	}

	if (::listen(s, backlog) != 0)
	{
		errno = async_win_error_to_errno(::WSAGetLastError());
		return -1;
	}

	return 0;
}

int tcp_acceptor::close()
{
	if (!impl_)
		return 0;
	impl_->cancel_token_->close();
	this->cancel();

	io_context *io = impl_->executor_.context();
	EnterCriticalSection(&impl_->mutex_);
	SOCKET s = impl_->socket_;
	impl_->socket_ = INVALID_SOCKET;
	LeaveCriticalSection(&impl_->mutex_);

	if (io)
		io->get_socket_service().unregister_socket(impl_);

	if (s != INVALID_SOCKET)
		::closesocket(s);

	return 0;
}

int tcp_acceptor::async_accept(void (*callback)(void *, async_error_code, SOCKET),
								   void *context)
{
	return this->async_accept(callback, context, nullptr);
}

int tcp_acceptor::async_accept(void (*callback)(void *, async_error_code, SOCKET),
								   void *context, void (*destroy)(void *))
{
	return this->async_accept(callback, context, cancellation_slot(), destroy);
}

int tcp_acceptor::async_accept(void (*callback)(void *, async_error_code, SOCKET),
								   void *context,
								   const cancellation_slot &slot,
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

	SOCKET listener;
	EnterCriticalSection(&impl_->mutex_);
	if (impl_->socket_ == INVALID_SOCKET)
	{
		LeaveCriticalSection(&impl_->mutex_);
		errno = EBADF;
		return -1;
	}
	listener = impl_->socket_;
	LeaveCriticalSection(&impl_->mutex_);

	int family = listener_family(listener);

	accept_op *op = (accept_op *)accept_op_alloc(impl_->executor_.context()->get_op_pools());
	if (!op)
	{
		errno = ENOMEM;
		return -1;
	}
	op->work_ = handler_work(impl_->executor_);
	op->listener_ = listener;
	op->accepted_ = INVALID_SOCKET;
	op->cancel_token_ = impl_->cancel_token_;
	op->cancel_token_->acquire();
	op->callback_ = callback;
	op->context_ = context;
	op->destroy_ = destroy;
	op->cancelled_ = 0;
	memset(op->output_buffer_, 0, sizeof op->output_buffer_);

	EnterCriticalSection(&impl_->mutex_);
	if (impl_->socket_ != listener)
	{
		LeaveCriticalSection(&impl_->mutex_);
		accept_op_free(op);
		errno = EBUSY;
		return -1;
	}
	LeaveCriticalSection(&impl_->mutex_);


	win_iocp_operation *iocp_op = op;
	if (slot.is_connected())
	{
		iocp_op_cancellation *cancellation =
			iocp_op_cancellation::create((HANDLE)listener, op, slot,
										 &op->cancelled_);
		if (!cancellation)
		{
			accept_op_free(op);
			return -1;
		}
		iocp_op = cancellation;
	}

	DWORD addr_len = (DWORD)(sizeof(sockaddr_storage) + 16);
	io->get_socket_service().start_accept_op(
		impl_, family, SOCK_STREAM, IPPROTO_TCP, &op->accepted_,
		op->output_buffer_, addr_len, iocp_op);
	return 0;
}

int tcp_acceptor::cancel()
{
	if (!impl_)
		return 0;
	EnterCriticalSection(&impl_->mutex_);
	SOCKET s = impl_->socket_;
	LeaveCriticalSection(&impl_->mutex_);

	return impl_->executor_.context()->get_socket_service().cancel(impl_);
}

SOCKET tcp_acceptor::native_handle() const
{
	return impl_ ? impl_->socket_ : INVALID_SOCKET;
}

executor tcp_acceptor::get_executor() const
{
	return impl_ ? impl_->executor_ : executor();
}

