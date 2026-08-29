#include "Communicator.h"
#include "comm/comm_conn.h"
#include "comm/comm_request_op.h"
#include "comm/comm_sleep_op.h"
#include "comm/comm_file_io_op.h"
#include "comm/comm_service_op.h"
#include "async/tcp_socket.h"
#include "async/tcp_acceptor.h"
#include "async/ssl_stream.h"
#include "async/udp_socket.h"
#include "async/steady_timer.h"
#include "async/strand.h"
#include "async/random_access_handle.h"
#include "async/error.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include <climits>
#include <new>

/* ---- upper business implementation ---- */

namespace
{

struct comm_reply_context
{
	CommConnEntry *entry;
	struct sockaddr_storage addr;
	int addrlen;
};

void comm_reply_destroy(void *context)
{
	comm_reply_context *reply =
		static_cast<comm_reply_context *>(context);
	reply->entry->release();
	free(reply);
}

void comm_reply_start(void *context)
{
	comm_reply_context *reply =
		static_cast<comm_reply_context *>(context);
	const struct sockaddr *addr = reply->addrlen != 0
		? reinterpret_cast<const struct sockaddr *>(&reply->addr) : nullptr;
	if (comm_request_op::start_reply(reply->entry, addr, reply->addrlen, true) < 0 &&
		::InterlockedCompareExchange(&reply->entry->state, 0, 0) ==
			CONN_STATE_RECEIVING)
	{
		/* The reservation can outlive the handler that made it if shutdown or
		 * retire wins before this strand command runs.  Consume that reservation
		 * through the normal cancellation path. */
		::InterlockedExchange(&reply->entry->state, CONN_STATE_CLOSING);
		comm_request_op::destroy_request(reply->entry);
	}
}

} /* namespace */


CommTarget::CommTarget()
	: addr(nullptr), addrlen(0), connect_timeout(0), response_timeout(0),
	  ssl_connect_timeout(0), ssl_ctx(nullptr),
	  transport_kind(COMM_TRANSPORT_TCP)
{
}

int CommTarget::init(const struct sockaddr *addr, socklen_t addrlen,
						 int connect_timeout, int response_timeout)
{
	if (!addr || addrlen <= 0)
	{
		errno = EINVAL;
		return -1;
	}
	this->addr = (struct sockaddr *)malloc(addrlen);
	if (!this->addr)
		return -1;
	memcpy(this->addr, addr, addrlen);
	this->addrlen = addrlen;
	this->connect_timeout = connect_timeout;
	this->response_timeout = response_timeout;
	this->ssl_connect_timeout = 0;
	this->ssl_ctx = NULL;
	InitializeSRWLock(&this->lock);
	INIT_LIST_HEAD(&this->idle_list);
	return 0;
}

void CommTarget::deinit()
{
	free(this->addr);
	this->addr = NULL;
	this->addrlen = 0;
}

void CommTarget::get_addr(const struct sockaddr **addr,
						  socklen_t *addrlen) const
{
	if (addr)
		*addr = this->addr;
	if (addrlen)
		*addrlen = this->addrlen;
}

int CommTarget::has_idle_conn() const
{
	CommTarget *target = const_cast<CommTarget *>(this);
	int has_idle;
	AcquireSRWLockShared(&target->lock);
	has_idle = !list_empty(&target->idle_list);
	ReleaseSRWLockShared(&target->lock);
	return has_idle;
}

void CommTarget::set_ssl(SSL_CTX *ssl_ctx, int ssl_connect_timeout)
{
	this->ssl_ctx = ssl_ctx;
	this->ssl_connect_timeout = ssl_connect_timeout;
}

SSL_CTX *CommTarget::get_ssl_ctx() const
{
	return this->ssl_ctx;
}

int CommTarget::transport() const
{
	return this->transport_kind;
}

void CommTarget::set_transport(int transport)
{
	this->transport_kind = transport;
}

SOCKET CommTarget::create_connect_socket()
{
	int type;
	int protocol;

	switch (this->transport())
	{
	case COMM_TRANSPORT_TCP:
		type = SOCK_STREAM;
		protocol = IPPROTO_TCP;
		break;
	case COMM_TRANSPORT_UDP:
		type = SOCK_DGRAM;
		protocol = IPPROTO_UDP;
		break;
	case COMM_TRANSPORT_SCTP:
		/* Stock Windows has no SCTP stream provider.  Do not silently
		 * turn an SCTP target into a TCP target. */
		errno = EPROTONOSUPPORT;
		return INVALID_SOCKET;
	default:
		errno = EINVAL;
		return INVALID_SOCKET;
	}

	return WSASocketW(this->addr->sa_family, type, protocol,
					  NULL, 0, WSA_FLAG_OVERLAPPED);
}

CommConnection *CommTarget::new_connection(SOCKET socket)
{
	(void)socket;
	return new (std::nothrow) CommConnection;
}

int CommTarget::init_ssl(SSL *ssl)
{
	(void)ssl;
	return 0;
}

void CommTarget::release()
{
}

CommTarget::~CommTarget()
{
}

int CommMessageIn::feedback(const void *buf, size_t size)
{
	/* feedback() is the synchronous Workflow compatibility operation.  The
	 * handle is non-blocking, so the return value is the bytes accepted by the
	 * native transport.  Ordinary request/reply writes remain asynchronous. */
	if (!this->entry || (!buf && size != 0))
	{
		errno = ENOENT;
		return -1;
	}
	if (size == 0)
		return 0;
	if (size > (size_t)INT_MAX)
	{
		errno = EOVERFLOW;
		return -1;
	}

	CommConnEntry *entry = this->entry;
	if (::InterlockedCompareExchange(&entry->retired, 0, 0))
	{
		errno = ENOENT;
		return -1;
	}

	int ret;
	if (entry->ssl_sock)
		/* SSLWrapper already converted the payload to ciphertext. */
		ret = entry->ssl_sock->write_transport_some(buf, size);
	else if (entry->udp_sock)
	{
		const struct sockaddr *addr;
		socklen_t addrlen;
		entry->target->get_addr(&addr, &addrlen);
		ret = entry->udp_sock->send_to(buf, size, addr, addrlen);
	}
	else if (entry->tcp)
		ret = entry->tcp->write_some(buf, size);
	else
	{
		errno = EBADF;
		return -1;
	}

	return ret;
}

void CommMessageIn::renew()
{
	if (!this->entry)
		return;

	comm_request_op *op = static_cast<comm_request_op *>(
		::InterlockedCompareExchangePointer(
			reinterpret_cast<PVOID volatile *>(&this->entry->request_op),
			nullptr, nullptr));
	if (op)
		::InterlockedExchange(&op->renew_requested_, 1);
}

CommMessageIn *CommMessageIn::inner()
{
	return this;
}

CommSession::CommSession()
{
	target = NULL;
	conn = NULL;
	out = NULL;
	in = NULL;
	seq = 0;
	passive = 0;
}

CommSession::~CommSession()
{
	if (this->passive)
	comm_request_op::destroy_session(this);
}

int CommService::init(const struct sockaddr *bind_addr, socklen_t addrlen,
					  int listen_timeout, int response_timeout)
{
	if (!bind_addr || addrlen <= 0)
	{
		errno = EINVAL;
		return -1;
	}
	this->bind_addr = (struct sockaddr *)malloc(addrlen);
	if (!this->bind_addr)
		return -1;
	memcpy(this->bind_addr, bind_addr, addrlen);
	this->addrlen = addrlen;
	this->listen_timeout = listen_timeout;
	this->response_timeout = response_timeout;
	this->ssl_accept_timeout = 0;
	this->ssl_ctx = NULL;
	this->reliable = 1;
	this->ref = 0;
	this->listener_handle = nullptr;
	this->recv_handle = nullptr;
	this->seq = 0;
	this->closing = 0;
	this->listener_released = 0;
	INIT_LIST_HEAD(&this->live_list);
	InitializeSRWLock(&this->lock);
	INIT_LIST_HEAD(&this->keep_alive_list);
	return 0;
}

void CommService::deinit()
{
	free(this->bind_addr);
	this->bind_addr = NULL;
	this->addrlen = 0;
}

int CommService::drain(int max)
{
	return comm_request_op::drain_service(this, max);
}

void CommService::get_addr(const struct sockaddr **addr,
						   socklen_t *addrlen) const
{
	if (addr)
		*addr = this->bind_addr;
	if (addrlen)
		*addrlen = this->addrlen;
}

void CommService::set_reliable(int reliable)
{
	this->reliable = reliable;
}

void CommService::set_ssl(SSL_CTX *ssl_ctx, int ssl_accept_timeout)
{
	this->ssl_ctx = ssl_ctx;
	this->ssl_accept_timeout = ssl_accept_timeout;
}

SSL_CTX *CommService::get_ssl_ctx() const
{
	return this->ssl_ctx;
}

SOCKET CommService::create_listen_socket()
{
	return WSASocketW(this->bind_addr->sa_family, SOCK_STREAM, IPPROTO_TCP,
					  NULL, 0, WSA_FLAG_OVERLAPPED);
}

SOCKET CommService::create_datagram_socket()
{
	return WSASocketW(this->bind_addr->sa_family, SOCK_DGRAM, IPPROTO_UDP,
					  NULL, 0, WSA_FLAG_OVERLAPPED);
}

CommConnection *CommService::new_connection(SOCKET socket)
{
	(void)socket;
	return new (std::nothrow) CommConnection;
}

int CommService::init_ssl(SSL *ssl)
{
	(void)ssl;
	return 0;
}

void CommService::handle_stop(int error)
{
	(void)error;
}

void CommService::incref()
{
	InterlockedIncrement(&this->ref);
}

void CommService::decref()
{
	if (InterlockedDecrement(&this->ref) == 0)
		this->handle_unbound();
}

CommService::~CommService()
{
}

SleepSession::~SleepSession()
{
}

/* ---- Communicator ---- */

Communicator::Communicator() : impl_(nullptr)
{
	void *mem = malloc(sizeof(CommunicatorImpl));
	if (mem)
		impl_ = new (mem) CommunicatorImpl;
	else
		errno = ENOMEM;
}

Communicator::~Communicator()
{
	deinit();
	if (impl_)
	{
		impl_->~CommunicatorImpl();
		free(impl_);
	}
}

int Communicator::init(size_t io_threads)
{
	return this->init(io_threads, io_threads ? io_threads : 1);
}
int Communicator::init(size_t io_threads, size_t handler_threads)
{
	if (!impl_)
	{
		errno = ENOMEM;
		return -1;
	}
	if (impl_->started)
	{
		errno = EALREADY;
		return -1;
	}

	if (impl_->kernel.init(io_threads) != 0)
		return -1;
	if (impl_->init_handler_pool(handler_threads) != 0)
	{
		int error = errno ? errno : ENOMEM;
		impl_->kernel.deinit();
		errno = error;
		return -1;
	}
	impl_->started = true;
	return 0;
}

void Communicator::deinit()
{
	if (impl_)
		impl_->shutdown();
}

int Communicator::is_handler_thread() const
{
	return impl_ &&
		thrdpool_in_pool(impl_->handler_pool) ? 1 : 0;
}

int Communicator::increase_handler_thread()
{
	if (!impl_ || !impl_->started)
	{
		errno = EINVAL;
		return -1;
	}
	return thrdpool_increase(impl_->handler_pool);
}

int Communicator::decrease_handler_thread()
{
	if (!impl_ || !impl_->started)
	{
		errno = EINVAL;
		return -1;
	}
	return thrdpool_decrease(impl_->handler_pool);
}

void Communicator::customize_event_handler(CommEventHandler *handler)
{
	if (!impl_)
		return;

	AcquireSRWLockExclusive(&impl_->lifecycle_lock);
	impl_->event_handler = handler;
	ReleaseSRWLockExclusive(&impl_->lifecycle_lock);
}

int Communicator::request(CommSession *session, CommTarget *target)
{
	if (!impl_ || !impl_->started || !session || !target || session->passive)
	{
		errno = EINVAL;
		return -1;
	}

	return comm_request_op::start_client(impl_, session, target);
}

int Communicator::reply(CommSession *session)
{
	if (!impl_ || !impl_->started || !session || !session->passive)
	{
		errno = EINVAL;
		return -1;
	}
	if (session->out || !session->in || !session->in->entry)
	{
		errno = ENOENT;
		return -1;
	}

	CommConnEntry *entry = session->in->entry;
	if (!entry->service || entry->session != session)
	{
		errno = ENOENT;
		return -1;
	}
	if (!entry->target)
	{
		errno = ENOENT;
		return -1;
	}
	/* reply() is called from the Workflow handler pool.  Reserve the logical
	 * response under the same target phase lock used by push().  The lock is
	 * held only for the claim; the actual operation is still created on the
	 * connection strand. */
	AcquireSRWLockExclusive(&entry->target->lock);
	bool claimable = entry->session == session &&
		!::InterlockedCompareExchange(&entry->retired, 0, 0);
	int state = claimable ? ::InterlockedCompareExchange(&entry->state,
		CONN_STATE_RECEIVING, CONN_STATE_IDLE) : CONN_STATE_CLOSING;
	ReleaseSRWLockExclusive(&entry->target->lock);
	if (!claimable || state != CONN_STATE_IDLE)
	{
		errno = ENOENT;
		return -1;
	}
	comm_reply_context *reply = static_cast<comm_reply_context *>(
		malloc(sizeof(*reply)));
	if (!reply)
	{
		AcquireSRWLockExclusive(&entry->target->lock);
		::InterlockedCompareExchange(&entry->state, CONN_STATE_IDLE,
			CONN_STATE_RECEIVING);
		ReleaseSRWLockExclusive(&entry->target->lock);
		errno = ENOMEM;
		return -1;
	}
	reply->entry = entry;
	reply->addrlen = entry->udp_sock ? entry->udp_fromlen : 0;
	if (reply->addrlen != 0)
		memcpy(&reply->addr, &entry->udp_from, (size_t)reply->addrlen);
	::InterlockedIncrement(&entry->refs);
	if (entry->serial.dispatch(&comm_reply_start, reply,
			&comm_reply_destroy) != 0)
	{
		int error = errno ? errno : EAGAIN;
		AcquireSRWLockExclusive(&entry->target->lock);
		::InterlockedCompareExchange(&entry->state, CONN_STATE_IDLE,
								 CONN_STATE_RECEIVING);
		ReleaseSRWLockExclusive(&entry->target->lock);
		comm_reply_destroy(reply);
		errno = error;
		return -1;
	}
	return 0;
}

int Communicator::push(const void *buf, size_t size, CommSession *session)
{
	if (!impl_ || !impl_->started || (!buf && size != 0) || !session || !session->in ||
		!session->in->entry)
	{
		errno = ENOENT;
		return -1;
	}

	CommConnEntry *entry = session->in->entry;
	if (!session->passive || !entry->service || !entry->target)
	{
		errno = ENOENT;
		return -1;
	}

	/* push() is the synchronous TOREPLY path.  The target lock is shared with
	 * reply()'s connection claim: it protects one short send phase, rather than
	 * turning feedback into an asynchronous write operation. */
	AcquireSRWLockExclusive(&entry->target->lock);
	if (entry->session != session ||
		::InterlockedCompareExchange(&entry->retired, 0, 0) ||
		::InterlockedCompareExchange(&entry->state, 0, 0) != CONN_STATE_IDLE)
	{
		ReleaseSRWLockExclusive(&entry->target->lock);
		errno = ENOENT;
		return -1;
	}

	int ret = session->in->inner()->feedback(buf, size);
	ReleaseSRWLockExclusive(&entry->target->lock);
	return ret;
}

int Communicator::shutdown(CommSession *session)
{
	if (!impl_ || !impl_->started || !session || !session->passive)
	{
		errno = EINVAL;
		return -1;
	}
	if (session->out || !session->in || !session->in->entry)
	{
		errno = ENOENT;
		return -1;
	}

	CommConnEntry *entry = session->in->entry;
	if (entry->session != session)
	{
		errno = ENOENT;
		return -1;
	}

	return comm_request_op::shutdown_request(entry);
}

int Communicator::bind(CommService *service)
{
	if (!impl_ || !impl_->started || !service ||
		service->listener_handle || service->recv_handle)
	{
		errno = EINVAL;
		return -1;
	}

	return comm_service_op::bind(impl_, service);
}

void Communicator::unbind(CommService *service)
{
	comm_service_op::unbind(service);
}


int Communicator::sleep(SleepSession *session)
{
	if (!impl_ || !session)
	{
		errno = EINVAL;
		return -1;
	}

	struct timespec value;
	if (session->duration(&value) < 0)
		return -1;

	long long ms = (long long)value.tv_sec * 1000 +
				   value.tv_nsec / 1000000;
	if (ms < 0)
		ms = 0;
	return comm_sleep_op::start(impl_, session, ms);
}

int Communicator::unsleep(SleepSession *session)
{
	if (!impl_ || !session)
	{
		errno = EINVAL;
		return -1;
	}

	return comm_sleep_op::cancel(session);
}

int Communicator::io_bind(IOService *service)
{
	if (!impl_ || !impl_->started || !service)
	{
		errno = EINVAL;
		return -1;
	}
	return impl_->bind_io_service(this, service);
}

void Communicator::io_unbind(IOService *service)
{
	if (!impl_ || !service)
		return;
	AcquireSRWLockExclusive(&service->bind_lock);
	if (service->owner.load(std::memory_order_acquire) == this)
	{
		service->owner = NULL;
		impl_->unbind_io_service(service);
	}
	bool idle = service->nevents.load(std::memory_order_acquire) == 0;
	ReleaseSRWLockExclusive(&service->bind_lock);
	if (idle)
		service->maybe_unbound();
}

int Communicator::io_request(IOSession *session)
{
	if (!impl_ || !impl_->started || !session)
	{
		errno = EINVAL;
		return -1;
	}

	return comm_file_io_op::start(impl_, session);
}
