#include "comm_request_op.h"

#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include <climits>
#include <new>

namespace
{

int comm_timeout_min(int first, int second)
{
	if (first < 0)
		return second;
	if (second < 0)
		return first;
	return first < second ? first : second;
}

/* ASIO stopped() discipline: once the connection is retired every pending
 * completion is an error, and no new operation may be started from a
 * completion handler.  Handlers observe the retired flag and complete the
 * operation with ECANCELED instead of continuing the flow. */
bool entry_retired(CommConnEntry *entry)
{
	return ::InterlockedCompareExchange(&entry->retired, 0, 0) != 0;
}

} /* namespace */

comm_request_op::comm_request_op()
	: entry_(nullptr), session_(nullptr), addr_storage_(), addr_(nullptr),
	  addrlen_(0), reuse_(false), kind_(REQUEST_CLIENT), keep_alive_(false),
	  entry_ref_(false), terminal_(false),
	  business_error_(0), renew_requested_(0)
{
	composed_op_init(this, &comm_request_op::destroy,
					 &comm_request_op::complete);
	composed_op_set_cancellation(this, cancellation_slot(),
							 &comm_request_op::cancel);
}

comm_request_op *comm_request_op::create(CommConnEntry *entry,
										 request_kind kind,
										 bool hold_entry)
{
	void *mem = malloc(sizeof(comm_request_op));
	if (!mem)
	{
		errno = ENOMEM;
		return nullptr;
	}

	comm_request_op *op = new (mem) comm_request_op();
	op->entry_ = entry;
	op->kind_ = kind;
	::InterlockedExchangePointer(
		reinterpret_cast<PVOID volatile *>(&entry->request_op), op);
	if (hold_entry)
	{
		::InterlockedIncrement(&entry->refs);
		op->entry_ref_ = true;
	}
	composed_op_set_executor(op, executor(entry->serial));
	return op;
}

void comm_request_op::destroy(composed_op *base)
{
	comm_request_op *self = static_cast<comm_request_op *>(base);
	if (self->entry_)
		::InterlockedCompareExchangePointer(
			reinterpret_cast<PVOID volatile *>(&self->entry_->request_op),
			nullptr, self);
	/* An operation destroyed before completion abandons the connection:
	 * retire it.  An operation that handed its connection to the persistent
	 * idle read (entry_ref_ == false) or that completed normally must not. */
	if (self->entry_ && self->entry_ref_)
	{
		self->entry_ref_ = false;
		if (::InterlockedCompareExchange(&self->completed_, 0, 0) == 0)
			retire_request(self->entry_, CS_STATE_STOPPED, ECANCELED, false);
		self->entry_->release();
	}
	self->~comm_request_op();
	free(self);
}

void comm_request_op::complete(composed_op *base)
{
	comm_request_op *self = static_cast<comm_request_op *>(base);
	int state = (int)self->result_socket_;
	CommSession *session = self->session_;
	if (self->kind_ == REQUEST_SERVER_RECEIVE)
	{
		if (self->terminal_)
		{
			if (session)
				session->handle(state, self->business_error_);
			return;
		}
		if (state == CS_STATE_TOREPLY)
		{
			self->session_ = nullptr;
			if (session)
				session->handle(CS_STATE_TOREPLY, self->business_error_);
		}
		else
		{
			self->session_ = nullptr;
			if (session)
				session->handle(state, self->business_error_);
		}
		return;
	}
	if (self->kind_ == REQUEST_SERVER_START)
	{
		/* The start actor does not own the session (the read loop does): a
		 * successful start keeps the connection resident without invoking
		 * the user handler; anything else retires it. */
		if (state == CS_STATE_SUCCESS && self->business_error_ == 0)
			return;
		self->session_ = nullptr;
		if (session)
			session->handle(state, self->business_error_);
		return;
	}
	if (!self->keep_alive_ || state != CS_STATE_SUCCESS ||
		self->business_error_ != 0)
	{
		self->session_ = nullptr;
		if (session)
			session->handle(state, self->business_error_);
		return;
	}
	self->session_ = nullptr;
	if (session)
		session->handle(state, self->business_error_);
}

void comm_request_op::cancel(composed_op *base, cancellation_type /*type*/)
{
	/* cancellation_state emits to the active child before this notification.
	 * The child operation owns transport and timer cancellation. */
	(void)base;
}

void comm_request_op::retire_request(CommConnEntry *entry, int state,
									 int error, bool notify, bool clear_input)
{
	if (::InterlockedExchange(&entry->retired, 1))
		return;
	if (!list_empty(&entry->list))
	{
		SRWLOCK *lock = entry->service ? &entry->service->lock :
			&entry->target->lock;
		AcquireSRWLockExclusive(lock);
		if (!list_empty(&entry->list))
		{
			list_del(&entry->list);
			INIT_LIST_HEAD(&entry->list);
		}
		ReleaseSRWLockExclusive(lock);
	}

	CommSession *session = entry->session;
	comm_request_op *notification = nullptr;
	if (notify && session)
	{
		notification = comm_request_op::create(entry,
			REQUEST_SERVER_RECEIVE, true);
		if (notification)
		{
			notification->session_ = session;
			notification->terminal_ = true;
		}
	}
	if (clear_input &&
		session && session->in &&
		session->in->entry == entry)
		session->in->entry = nullptr;
	entry->session = nullptr;
	entry->close_transport();

	if (entry->target)
		entry->target->release();
	if (notify && session)
	{
		if (notification)
		{
			notification->result_socket_ = (UINT_PTR)state;
			notification->business_error_ = error;
			notification->result_error_ = async_error_from_errno(error);
			::InterlockedExchange(&notification->completed_, 1);
			comm_request_op::post_completion(notification);
			composed_op_release(notification);
		}
		else
		{
			/* A terminal business callback must remain in the handler domain.  An
			 * operation allocation failure cannot be repaired by running it on the
			 * IOCP worker, so treat it as an internal allocation invariant failure. */
			assert(!"unable to allocate terminal handler operation");
			RaiseFailFastException(nullptr, nullptr, 0);
		}
	}
	else
		entry->destroy_connection();

	/* The owner reference is separate from child-operation references.  It can
	 * be released during shutdown because the drain reference keeps entry alive
	 * until AsyncCore has retired every child operation. */
	entry->release_owner();
}

void comm_request_op::finish(comm_request_op *self, int state, int error)
{
	if (!composed_op_try_complete(self))
		return;
	CommConnEntry *entry = self->entry_;
	self->session_ = entry->session;
	self->result_socket_ = (UINT_PTR)state;
	self->business_error_ = error;
	self->result_error_ = async_error_from_errno(error);

	/* The state transition and all ASIO-owned cleanup happen before the
	 * Workflow handler is queued.  A successful server start and a TOREPLY
	 * request keep the connection resident; a keep-alive request has already
	 * parked the entry.  Every other terminal result retires the entry here. */
	if (self->kind_ == REQUEST_SERVER_RECEIVE && state == CS_STATE_TOREPLY &&
		error == 0)
	{
		::InterlockedExchange(&entry->state, CONN_STATE_IDLE);
	}
	else if (self->kind_ == REQUEST_SERVER_START &&
			 state == CS_STATE_SUCCESS && error == 0)
	{
		/* The server read loop owns the resident entry. */
	}
	else if (self->keep_alive_ && state == CS_STATE_SUCCESS && error == 0)
	{
		CommSession *session = self->session_;
		if (entry->session == session)
			entry->session = nullptr;
		if (session && session->in && session->in->entry == entry)
			session->in->entry = nullptr;
		/* Client targets are held by the request while the entry is active;
		 * the server target remains owned by the persistent service entry. */
		if (!entry->service && entry->target)
			entry->target->release();
	}
	else
	{
		comm_request_op::retire_io(self, state, error);
	}

	/* The transport phase is over.  Do not keep ASIO's IO/strand work alive
	 * while the Workflow completion waits in the separate handler pool. */
	self->work_.reset();
	comm_request_op::post_completion(self);
}

void comm_request_op::retire_io(comm_request_op *self, int state, int error)
{
	if (!self || !self->entry_)
		return;

	/* finish() has already captured the session pointer.  This call only
	 * retires ASIO/transport ownership; the handler stage reports the frozen
	 * result later. */
	comm_request_op::retire_request(self->entry_, state, error, false, true);
}

void comm_request_op::handle_complete(void *ctx)
{
	comm_request_op *self = static_cast<comm_request_op *>(ctx);
	composed_op_complete_handler(self);
	composed_op_release(self);
}

void comm_request_op::post_completion(comm_request_op *self)
{
	::InterlockedExchange(&self->result_ready_, 1);
	::InterlockedExchange(&self->dispatch_started_, 1);
	composed_op_add_ref(self);
	int ret = self->entry_->impl->post_handler(
			&comm_request_op::handle_complete, self, &self->handler_task_);
	/* Accepted operations are drained before the handler pool is destroyed.
	 * A failed post is an internal lifecycle violation, not an IOCP-thread
	 * completion path. */
	assert(ret == 0);
	if (ret != 0)
		RaiseFailFastException(nullptr, nullptr, 0);
}

void comm_request_op::destroy_request(CommConnEntry *entry)
{
	if (!entry)
		return;

	::InterlockedIncrement(&entry->refs);
	if (entry->serial.post(&comm_request_op::cancel_entry, entry,
			&comm_request_op::release_entry) != 0)
	{
		entry->release();
		retire_request(entry, CS_STATE_STOPPED, 0, false);
	}
}

void comm_request_op::release_entry(void *context)
{
	static_cast<CommConnEntry *>(context)->release();
}

void comm_request_op::cancel_entry(void *context)
{
	CommConnEntry *entry = static_cast<CommConnEntry *>(context);
	/* Client pooled entries have only an expiry timer.  Server keep-alive is
	 * represented by the current message-read operation below. */
	if (::InterlockedExchangeAdd(&entry->idle_timer_active, 0))
	{
		entry->idle_timer.cancel();
		retire_idle_entry(entry);
		entry->release();
		return;
	}
	comm_request_op *op = entry->request_op;
	if (op && ::InterlockedExchangeAdd(&op->completed_, 0) == 0)
		composed_op_cancel(op, cancellation_type::terminal);
	else
		retire_request(entry, CS_STATE_STOPPED, 0, false);
	entry->release();
}

int comm_request_op::shutdown_request(CommConnEntry *entry)
{
	if (!entry || !entry->session ||
		::InterlockedCompareExchange(&entry->state, CONN_STATE_CLOSING,
									 CONN_STATE_IDLE) != CONN_STATE_IDLE)
	{
		errno = ENOENT;
		return -1;
	}

	destroy_request(entry);
	return 0;
}

void comm_request_op::destroy_session(CommSession *session)
{
	CommConnEntry *entry = session->in ? session->in->entry : nullptr;
	if (!session->out && entry && entry->session == session &&
		::InterlockedCompareExchange(&entry->state, CONN_STATE_CLOSING,
									 CONN_STATE_IDLE) == CONN_STATE_IDLE)
	{
		destroy_request(entry);
	}

	if (session->target)
		session->target->release();
}

int comm_request_op::client_ssl_init(SSL *ssl, void *userdata)
{
	CommTarget *target = static_cast<CommTarget *>(userdata);
	return target->init_ssl(ssl);
}

int comm_request_op::server_ssl_init(SSL *ssl, void *userdata)
{
	CommService *service = static_cast<CommService *>(userdata);
	return service->init_ssl(ssl);
}

int comm_request_op::create_server_session(CommConnEntry *entry)
{
	CommSession *session = entry->service->new_session(entry->seq, entry->conn);
	if (!session)
	{
		errno = ENOMEM;
		return -1;
	}
	session->passive = 1;
	session->target = entry->target;
	session->conn = entry->conn;
	session->out = nullptr;
	session->in = nullptr;
	session->seq = entry->seq++;
	static_cast<CommServiceTarget *>(entry->target)->incref();
	entry->session = session;
	return 0;
}

void comm_request_op::complete_server_reply(comm_request_op *self)
{
	CommConnEntry *entry = self->entry_;
	CommSession *session = entry->session;
	int keep_alive = session->keep_alive_timeout();
	if (keep_alive > 0 &&
		!::InterlockedCompareExchange(&entry->service->closing, 0, 0))
	{
		self->keep_alive_ = true;
		/* finish() snapshots and detaches the completed session.  Start the
		 * next server read only afterwards, so its session cannot be reported
		 * as the completion of the previous reply. */
		finish(self, CS_STATE_SUCCESS, 0);
		/* ASIO server sessions create a fresh session for every read cycle. */
		if (create_server_session(entry) != 0 ||
			start_server_read(entry, keep_alive) != 0)
			retire_request(entry, CS_STATE_ERROR, errno ? errno : EIO,
						   true);
		return;
	}
	finish(self, CS_STATE_SUCCESS, 0);
}

void comm_request_op::retire_idle_entry(CommConnEntry *entry)
{
	if (::InterlockedExchange(&entry->retired, 1))
		return;
	CommSession *session = entry->session;
	SRWLOCK *lock = entry->service ? &entry->service->lock :
		&entry->target->lock;
	AcquireSRWLockExclusive(lock);
	if (!list_empty(&entry->list))
	{
		list_del(&entry->list);
		INIT_LIST_HEAD(&entry->list);
	}
	ReleaseSRWLockExclusive(lock);
	if (session && session->in && session->in->entry == entry)
		session->in->entry = nullptr;
	entry->session = nullptr;
	entry->close_transport();
	if (entry->service && entry->target)
		entry->target->release();
	entry->destroy_connection();
	entry->release_owner();
}

int comm_request_op::start_reply(CommConnEntry *entry,
								 const struct sockaddr *addr, int addrlen,
								 bool reserved)
{
	if (!entry || !entry->impl || !entry->session || !entry->service)
	{
		errno = ENOENT;
		return -1;
	}
	if (reserved)
	{
		if (::InterlockedCompareExchange(&entry->state, 0, 0) !=
			CONN_STATE_RECEIVING)
		{
			errno = ENOENT;
			return -1;
		}
	}
	else
	{
		/* push() holds the same target lock while performing its synchronous
		 * feedback send.  Claim the TOREPLY phase under that lock so reply either
		 * waits for that short send or wins the phase before a later push. */
		AcquireSRWLockExclusive(&entry->target->lock);
		int state = ::InterlockedCompareExchange(&entry->state,
			CONN_STATE_RECEIVING, CONN_STATE_IDLE);
		ReleaseSRWLockExclusive(&entry->target->lock);
		if (state != CONN_STATE_IDLE)
		{
			errno = ENOENT;
			return -1;
		}
	}
	if (addrlen < 0 || (size_t)addrlen > sizeof(struct sockaddr_storage) ||
		(addrlen != 0 && !addr))
	{
		errno = EINVAL;
		return -1;
	}

	comm_request_op *op = comm_request_op::create(
		entry, REQUEST_SERVER_REPLY, true);
	if (!op)
	{
		::InterlockedExchange(&entry->state, CONN_STATE_CLOSING);
		comm_request_op::destroy_request(entry);
		return -1;
	}
	if (addrlen != 0)
	{
		memcpy(&op->addr_storage_, addr, (size_t)addrlen);
		op->addr_ = reinterpret_cast<const struct sockaddr *>(
			&op->addr_storage_);
		op->addrlen_ = addrlen;
	}
	op->reuse_ = false;

	comm_request_op::start_flow(op);
	composed_op_release(op);
	return 0;
}

int comm_request_op::start_server(CommConnEntry *entry)
{
	if (!entry || !entry->impl || !entry->service || !entry->conn ||
		(!entry->tcp && !entry->ssl_sock))
	{
		errno = EINVAL;
		return -1;
	}

	comm_request_op *op = comm_request_op::create(
		entry, REQUEST_SERVER_START, true);
	if (!op)
		return -1;
	/* The entry stays resident, owned by the accept path and the read-loop
	 * operations (ASIO server.cpp session).  The start actor holds an entry
	 * reference until it has handed the connection to the read loop: a
	 * successful start completes without retiring, and any failure, including
	 * a rejected completion dispatch, still retires the connection through
	 * destroy(). */
	op->keep_alive_ = true;

	int ret = 0;
	if (entry->ssl_sock)
	{
		composed_op_add_ref(op);
		ret = timed_handshake_start(entry->ssl_sock, &entry->output_timer,
			entry->service->ssl_accept_timeout,
			&comm_request_op::server_handshake_cb, op,
			composed_op_cancellation_slot(op),
			&comm_request_op::child_destroy);
		if (ret != 0)
		{
			int error = errno ? errno : EIO;
			composed_op_release(op);
			comm_request_op::finish(op, CS_STATE_ERROR, error);
		}
	}
	else
	{
		/* No handshake: enter the read loop directly.  The first read is the
		 * first iteration (ASIO server.cpp read_line chain).  The start actor
		 * completes successfully without retiring or invoking the user
		 * handler: the read loop owns the session from here on. */
		if (create_server_session(entry) < 0 ||
			start_server_read(entry, 0) != 0)
			comm_request_op::finish(op, CS_STATE_ERROR,
				errno ? errno : EIO);
		else
			comm_request_op::finish(op, CS_STATE_SUCCESS, 0);
	}

	composed_op_release(op);
	return 0;
}

int comm_request_op::start_server_datagram(CommConnEntry *entry,
									const void *buffer, size_t bytes)
{
	if (!entry || !entry->impl || !entry->service || !entry->conn ||
		!entry->udp_sock || !buffer || bytes == 0)
	{
		errno = EINVAL;
		return -1;
	}

	comm_request_op *op = comm_request_op::create(
		entry, REQUEST_SERVER_RECEIVE, true);
	if (!op)
		return -1;

	int ret = create_server_session(entry);
	if (ret == 0)
	{
		ret = prepare_message_in(entry);
		if (ret == 0)
		{
			size_t consumed = bytes;
			int append_ret = entry->session->in->append(buffer, &consumed);
			if (append_ret <= 0)
				ret = append_ret < 0 ? (errno ? errno : EIO) : EIO;
			else
				ret = 0;
		}
	}
	if (ret != 0)
	{
		comm_request_op::finish(op, CS_STATE_ERROR, ret);
	}
	else
	{
		::InterlockedExchange(&entry->state, CONN_STATE_IDLE);
		comm_request_op::finish(op, CS_STATE_TOREPLY, 0);
	}

	composed_op_release(op);
	return 0;
}

int comm_request_op::request_idle_entry(CommSession *session,
										 CommTarget *target)
{
	CommConnEntry *entry = nullptr;
	AcquireSRWLockExclusive(&target->lock);
	while (!list_empty(&target->idle_list))
	{
		struct list_head *pos = target->idle_list.next;
		CommConnEntry *candidate = list_entry(pos, CommConnEntry, list);
		list_del(pos);
		INIT_LIST_HEAD(pos);
		if (::InterlockedCompareExchange(&candidate->state,
				CONN_STATE_REUSING, CONN_STATE_KEEPALIVE) ==
			CONN_STATE_KEEPALIVE)
		{
			entry = candidate;
			break;
		}
	}
	ReleaseSRWLockExclusive(&target->lock);
	if (!entry)
	{
		errno = ENOENT;
		return -1;
	}

	entry->session = session;
	session->target = target;
	session->conn = entry->conn;
	session->out = nullptr;
	session->in = nullptr;
	session->seq = entry->seq++;

	/* The pool reference remains owned by the idle timer.  Keep a separate
	 * activation reference while the connection is handed to its strand. */
	::InterlockedIncrement(&entry->refs);
	if (entry->serial.post(&comm_request_op::activate_client_entry, entry,
			&comm_request_op::release_entry) != 0)
	{
		int error = errno ? errno : EIO;
		entry->release();
		retire_request(entry, CS_STATE_ERROR, error, true);
		return 0;
	}
	return 0;
}

void comm_request_op::activate_client_entry(void *context)
{
	CommConnEntry *entry = static_cast<CommConnEntry *>(context);
	if (entry_retired(entry))
		return;
	if (entry->impl &&
		::InterlockedCompareExchange(&entry->impl->shutting_down, 0, 0))
	{
		retire_request(entry, CS_STATE_STOPPED, ECANCELED, true);
		return;
	}

	/* The client idle timer owns only expiry.  The timer completion still owns
	 * the pool reference until it is destroyed, but it is no longer an active
	 * idle role once activation begins.  Clear the role before starting the
	 * request so a later cancel is routed to the request operation. */
	::InterlockedExchange(&entry->idle_timer_active, 0);
	entry->idle_timer.cancel();
	::InterlockedExchange(&entry->state, CONN_STATE_CONNECTED);
	if (comm_request_start(entry, nullptr, 0, true) != 0)
		retire_request(entry, CS_STATE_ERROR, errno ? errno : EIO, true);
}

int comm_request_op::start_client(CommunicatorImpl *impl,
								  CommSession *session, CommTarget *target)
{
	if (!impl || !session || !target || session->passive)
	{
		errno = EINVAL;
		return -1;
	}

	const struct sockaddr *addr;
	socklen_t addrlen;
	target->get_addr(&addr, &addrlen);
	if (!addr || addrlen <= 0)
	{
		errno = EINVAL;
		return -1;
	}

	int transport = target->transport();
	if (transport != COMM_TRANSPORT_TCP &&
		transport != COMM_TRANSPORT_UDP &&
		transport != COMM_TRANSPORT_SCTP)
	{
		errno = EINVAL;
		return -1;
	}

	int error_bak = errno;
	if (transport == COMM_TRANSPORT_TCP &&
		request_idle_entry(session, target) == 0)
	{
		errno = error_bak;
		return 0;
	}
	errno = error_bak;

	SOCKET socket;
	socket = target->create_connect_socket();
	if (socket == INVALID_SOCKET)
		return -1;

	CommConnection *connection = target->new_connection(socket);
	if (!connection)
	{
		::closesocket(socket);
		errno = ENOMEM;
		return -1;
	}

	CommConnEntry *entry = CommConnEntry::create(
		impl, &impl->kernel.get_io_context(), session, connection, target);
	if (!entry || !impl->register_entry(entry))
	{
		if (entry)
		{
			entry->destroy_connection();
			entry->release();
		}
		else
			delete connection;
		::closesocket(socket);
		errno = entry ? ECANCELED : ENOMEM;
		return -1;
	}

	session->target = target;
	session->conn = connection;
	session->out = nullptr;
	session->in = nullptr;
	session->seq = entry->seq++;

	int ret;
	switch (transport)
	{
	case COMM_TRANSPORT_UDP:
		ret = entry->construct_udp();
		if (ret == 0)
			ret = entry->udp_sock->assign(socket);
		break;
	case COMM_TRANSPORT_TCP:
		if (target->get_ssl_ctx())
		{
			ret = entry->construct_ssl(target->get_ssl_ctx(), 0);
			if (ret == 0)
				ret = entry->ssl_sock->assign(socket);
			if (ret == 0)
				entry->ssl_sock->set_init_callback(
					&comm_request_op::client_ssl_init, target);
		}
		else
		{
			ret = entry->construct_tcp();
			if (ret == 0)
				ret = entry->tcp->assign(socket);
		}
		break;
	case COMM_TRANSPORT_SCTP:
		/* There is no stock Windows ASIO stream handle for SCTP. */
		errno = EPROTONOSUPPORT;
		ret = -1;
		break;
	default:
		errno = EINVAL;
		ret = -1;
		break;
	}

	if (ret != 0)
	{
		int error = errno ? errno : EIO;
		SOCKET owned = INVALID_SOCKET;
		if (entry->tcp)
			owned = entry->tcp->native_handle();
		else if (entry->ssl_sock)
			owned = entry->ssl_sock->native_handle();
		else if (entry->udp_sock)
			owned = entry->udp_sock->native_handle();
		if (owned != socket)
			::closesocket(socket);
		entry->destroy_transport();
		entry->destroy_connection();
		session->conn = nullptr;
		entry->release();
		errno = error;
		return -1;
	}

	ret = comm_request_start(entry, addr, addrlen, false);
	if (ret != 0)
	{
		int error = errno ? errno : EIO;
		entry->destroy_transport();
		entry->destroy_connection();
		session->conn = nullptr;
		entry->release();
		errno = error;
		return -1;
	}
	return 0;
}

int comm_request_op::drain_service(CommService *service, int max)
{
	if (!service)
	{
		errno = EINVAL;
		return -1;
	}

	int count = 0;
	AcquireSRWLockExclusive(&service->lock);
	while (count != max && !list_empty(&service->keep_alive_list))
	{
		struct list_head *pos = service->keep_alive_list.prev;
		CommConnEntry *entry = list_entry(pos, CommConnEntry, list);
		list_del(pos);
		INIT_LIST_HEAD(pos);
		++count;
		destroy_request(entry);
	}
	ReleaseSRWLockExclusive(&service->lock);
	return count;
}

int comm_request_op::prepare_message_out(CommConnEntry *entry,
										 struct iovec *vectors, int *count)
{
	entry->session->out = entry->session->message_out();
	if (!entry->session->out)
	{
		errno = ENOSYS;
		return -1;
	}

	int cnt = entry->session->out->encode(vectors, V6_ENCODE_IOV_MAX);
	if ((unsigned int)cnt > V6_ENCODE_IOV_MAX)
	{
		if (cnt > V6_ENCODE_IOV_MAX)
			errno = EOVERFLOW;
		return -1;
	}
	*count = cnt;
	return 0;
}

int comm_request_op::prepare_message_in(CommConnEntry *entry)
{
	entry->session->in = entry->session->message_in();
	if (!entry->session->in)
	{
		errno = EINVAL;
		return -1;
	}

	entry->session->in->entry = entry;
	::InterlockedExchange(&entry->state, CONN_STATE_RECEIVING);
	return 0;
}

int comm_request_op::park_client_entry(CommConnEntry *entry, int timeout)
{
	if (timeout == 0)
		return 0;

	/* The idle timer owns an entry reference until expiry or cancellation. */
	::InterlockedIncrement(&entry->refs);
	AcquireSRWLockExclusive(&entry->target->lock);
	::InterlockedExchange(&entry->state, CONN_STATE_KEEPALIVE);
	list_add_tail(&entry->list, &entry->target->idle_list);
	ReleaseSRWLockExclusive(&entry->target->lock);

	int ret = start_idle_timer(entry, timeout);
	if (ret == 0)
		return 1;

	AcquireSRWLockExclusive(&entry->target->lock);
	if (!list_empty(&entry->list))
	{
		list_del(&entry->list);
		INIT_LIST_HEAD(&entry->list);
	}
	ReleaseSRWLockExclusive(&entry->target->lock);
	entry->release();
	return 0;
}

int comm_request_op::start_idle_timer(CommConnEntry *entry, int timeout)
{
	::InterlockedExchange(&entry->idle_timer_active, 1);
	entry->idle_timer.expires_after(std::chrono::milliseconds(timeout));
	if (entry->idle_timer.async_wait(&comm_request_op::idle_timer_cb, entry,
			&comm_request_op::idle_timer_destroy) != 0)
	{
		::InterlockedExchange(&entry->idle_timer_active, 0);
		return -1;
	}
	return 0;
}

void comm_request_op::idle_timer_cb(void *ctx, async_error_code error)
{
	CommConnEntry *entry = static_cast<CommConnEntry *>(ctx);
	::InterlockedExchange(&entry->idle_timer_active, 0);
	if (error || entry_retired(entry))
		return;
	if (::InterlockedCompareExchange(&entry->state, 0, 0) ==
		CONN_STATE_KEEPALIVE)
		retire_idle_entry(entry);
}

void comm_request_op::idle_timer_destroy(void *ctx)
{
	static_cast<CommConnEntry *>(ctx)->release();
}

int comm_request_op::start_send(comm_request_op *self)
{
	CommConnEntry *entry = self->entry_;
	struct iovec vectors[V6_ENCODE_IOV_MAX];
	int cnt;
	if (prepare_message_out(entry, vectors, &cnt) < 0)
		return -1;

	int response_timeout = entry->target ? entry->target->response_timeout :
		(entry->service ? entry->service->response_timeout : -1);
	int timeout = comm_timeout_min(response_timeout,
		entry->session->send_timeout());
	if (entry->udp_sock)
	{
		if (cnt == 0)
		{
			if (entry->service)
				finish(self, CS_STATE_SUCCESS, 0);
			else if (start_udp_read(self) < 0)
				finish(self, CS_STATE_ERROR, errno ? errno : EIO);
			return 0;
		}
		composed_op_add_ref(self);
		int ret = timed_udp_send_start(entry->udp_sock, &entry->output_timer,
			vectors, cnt, self->addr_, self->addrlen_, timeout,
			&comm_request_op::udp_send_cb, self,
			composed_op_cancellation_slot(self),
			&comm_request_op::child_destroy);
		if (ret < 0)
		{
			composed_op_release(self);
			return -1;
		}
		return 0;
	}

	size_t total = 0;
	for (int i = 0; i < cnt; ++i)
	{
		if (total > (size_t)-1 - vectors[i].iov_len)
		{
			errno = EOVERFLOW;
			return -1;
		}
		total += vectors[i].iov_len;
	}

	if (total == 0)
	{
		return start_read(self);
	}

	composed_op_add_ref(self);
	if (!entry->ssl_sock)
	{
		int ret = timed_write_start(entry->tcp, &entry->output_timer,
								 vectors, cnt, timeout,
								 &comm_request_op::write_cb, self,
								 composed_op_cancellation_slot(self),
								 &comm_request_op::child_destroy);
		if (ret < 0)
		{
			composed_op_release(self);
			return -1;
		}
		return 0;
	}

	int ret = timed_ssl_write_start(entry->ssl_sock, &entry->output_timer,
								 vectors, cnt, timeout,
								 &comm_request_op::write_cb, self,
								 composed_op_cancellation_slot(self),
								 &comm_request_op::child_destroy);
	if (ret < 0)
	{
		composed_op_release(self);
		return -1;
	}
	return 0;
}

int comm_request_op::start_read(comm_request_op *self)
{
	return start_message_read(self, &comm_request_op::read_cb,
								 &comm_request_op::read_destroy, 0);
}

int comm_request_op::start_udp_read(comm_request_op *self)
{
	CommConnEntry *entry = self->entry_;
	if (prepare_message_in(entry) < 0)
		return -1;
	int response_timeout = entry->target ? entry->target->response_timeout : -1;
	int first_timeout = entry->session->first_timeout();
	int timeout = first_timeout != 0 ? first_timeout :
		comm_timeout_min(response_timeout, entry->session->receive_timeout());
	composed_op_add_ref(self);
	int ret = async_recvfrom_message_ex(entry->udp_sock, nullptr,
		V6_READ_BUFSIZE, nullptr, nullptr,
		&comm_request_op::read_message_filter, entry,
		&comm_request_op::read_cb, self, timeout,
		composed_op_cancellation_slot(self),
		&comm_request_op::read_destroy);
	if (ret < 0)
	{
		composed_op_release(self);
		return -1;
	}
	return 0;
}

int comm_request_op::read_message_filter(void *buffer, size_t *size,
										 void *userdata)
{
	CommConnEntry *entry = static_cast<CommConnEntry *>(userdata);
	CommMessageIn *in = entry->session->in;
	if (!in)
	{
		errno = EINVAL;
		return -1;
	}

	size_t bytes = *size;
	int ret = in->append(buffer, size);
	if (ret > 0 && bytes > *size)
	{
		errno = EBADMSG;
		return -1;
	}
	return ret;
}

int comm_request_op::start_message_read(
	comm_request_op *self,
	void (*callback)(void *, async_error_code, size_t),
	void (*destroy)(void *), int first_timeout_override)
{
	CommConnEntry *entry = self->entry_;
	if (comm_request_op::prepare_message_in(entry) < 0)
		return -1;
	::InterlockedExchange(&self->renew_requested_, 0);
	int response_timeout = entry->target ? entry->target->response_timeout
								: (entry->service ? entry->service->response_timeout : 0);
	int first_timeout = entry->service ? first_timeout_override :
		entry->session->first_timeout();
	int receive_timeout = entry->session->receive_timeout();
	composed_op_add_ref(self);
	int ret;
	if (entry->ssl_sock)
		ret = async_read_message_ssl_ex(
			entry->ssl_sock, nullptr, V6_READ_BUFSIZE,
			&comm_request_op::read_message_filter, entry,
			callback, self,
			first_timeout, response_timeout, receive_timeout,
			composed_op_cancellation_slot(self),
			destroy, &self->renew_requested_);
	else
		ret = async_read_message_ex(
			entry->tcp, nullptr, V6_READ_BUFSIZE,
			&comm_request_op::read_message_filter, entry,
			callback, self,
			first_timeout, response_timeout, receive_timeout,
			composed_op_cancellation_slot(self),
			destroy, &self->renew_requested_);
	if (ret < 0)
	{
		composed_op_release(self);
		return -1;
	}
	return 0;
}

int comm_request_op::start_server_read(CommConnEntry *entry, int first_timeout)
{
	comm_request_op *op = comm_request_op::create(
		entry, REQUEST_SERVER_RECEIVE, true);
	if (!op)
		return -1;

	if (start_message_read(op, &comm_request_op::server_read_cb,
			&comm_request_op::server_read_destroy, first_timeout) < 0)
	{
		/* Initiation failure is synchronous.  The caller owns the business
		 * error path; this operation has not been submitted and must not also
		 * synthesize an asynchronous completion. */
		composed_op_release(op);
		return -1;
	}
	AcquireSRWLockExclusive(&entry->service->lock);
	if (list_empty(&entry->list))
		list_add_tail(&entry->list, &entry->service->keep_alive_list);
	ReleaseSRWLockExclusive(&entry->service->lock);

	composed_op_release(op);
	return 0;
}

void comm_request_op::server_read_cb(void *ctx, async_error_code error_code,
									 size_t /*bytes*/)
{
	int error = async_error_to_errno(error_code);
	comm_request_op *self = static_cast<comm_request_op *>(ctx);
	CommConnEntry *entry = self->entry_;
	AcquireSRWLockExclusive(&entry->service->lock);
	if (!list_empty(&entry->list))
	{
		list_del(&entry->list);
		INIT_LIST_HEAD(&entry->list);
	}
	ReleaseSRWLockExclusive(&entry->service->lock);
	if (error || entry_retired(self->entry_))
		finish(self, CS_STATE_ERROR, error ? error : ECANCELED);
	else
		finish(self, CS_STATE_TOREPLY, 0);
	/* Release the read child's reference (server_start_read's
	 * composed_op_add_ref): see read_cb. */
	composed_op_release(self);
}

void comm_request_op::server_read_destroy(void *ctx)
{
	/* Abort path of the server read child: see read_destroy. */
	composed_op_release(static_cast<comm_request_op *>(ctx));
}

void comm_request_op::start_flow(comm_request_op *self)
{
	CommConnEntry *entry = self->entry_;
	if (self->kind_ == REQUEST_SERVER_REPLY)
	{
		::InterlockedExchange(&entry->state, CONN_STATE_RECEIVING);
		if (start_send(self) < 0)
			finish(self, CS_STATE_ERROR, errno ? errno : EIO);
		return;
	}

	if (self->reuse_ || entry->udp_sock)
	{
		::InterlockedExchange(&entry->state, CONN_STATE_CONNECTED);
		if (start_send(self) < 0)
			finish(self, CS_STATE_ERROR, errno ? errno : EIO);
		return;
	}

	::InterlockedExchange(&entry->state, CONN_STATE_CONNECTING);
	composed_op_add_ref(self);
	int ret = entry->ssl_sock
		? timed_ssl_connect_start(entry->ssl_sock, &entry->output_timer,
			self->addr_, self->addrlen_, entry->target->connect_timeout,
			&comm_request_op::connect_cb, self,
			composed_op_cancellation_slot(self), &comm_request_op::child_destroy)
		: timed_connect_start(entry->tcp, &entry->output_timer,
			self->addr_, self->addrlen_, entry->target->connect_timeout,
			&comm_request_op::connect_cb, self,
			composed_op_cancellation_slot(self), &comm_request_op::child_destroy);
	if (ret < 0)
	{
		composed_op_release(self);
		finish(self, CS_STATE_ERROR, errno ? errno : EIO);
	}
}

void comm_request_op::connect_cb(void *ctx, async_error_code error_code)
{
	int error = async_error_to_errno(error_code);
	comm_request_op *self = static_cast<comm_request_op *>(ctx);
	CommConnEntry *entry = self->entry_;
	if (error || entry_retired(entry))
		finish(self, CS_STATE_ERROR, error ? error : ECANCELED);
	else
	{
		::InterlockedExchange(&entry->state, CONN_STATE_CONNECTED);
		if (entry->ssl_sock)
		{
			composed_op_add_ref(self);
			if (timed_handshake_start(entry->ssl_sock, &entry->output_timer,
					entry->target->ssl_connect_timeout,
					&comm_request_op::handshake_cb, self,
					composed_op_cancellation_slot(self),
					&comm_request_op::child_destroy) < 0)
			{
				composed_op_release(self);
				finish(self, CS_STATE_ERROR, errno ? errno : EIO);
			}
		}
		else if (start_send(self) < 0)
			finish(self, CS_STATE_ERROR, errno ? errno : EIO);
	}
}

void comm_request_op::handshake_cb(void *ctx, async_error_code error_code)
{
	int error = async_error_to_errno(error_code);
	comm_request_op *self = static_cast<comm_request_op *>(ctx);
	if (error || entry_retired(self->entry_))
		finish(self, CS_STATE_ERROR, error ? error : ECANCELED);
	else if (start_send(self) < 0)
		finish(self, CS_STATE_ERROR, errno ? errno : EIO);
}

void comm_request_op::server_handshake_cb(void *ctx,
									  async_error_code error_code)
{
	int error = async_error_to_errno(error_code);
	comm_request_op *self = static_cast<comm_request_op *>(ctx);
	CommConnEntry *entry = self->entry_;
	if (error || entry_retired(entry))
		finish(self, CS_STATE_ERROR, error ? error : ECANCELED);
	else
	{
		::InterlockedExchange(&entry->state, CONN_STATE_CONNECTED);
		/* Enter the read loop; the start actor completes successfully without
		 * retiring or invoking the user handler. */
		if (create_server_session(entry) < 0 ||
			start_server_read(entry, 0) != 0)
			finish(self, CS_STATE_ERROR, errno ? errno : EIO);
		else
			finish(self, CS_STATE_SUCCESS, 0);
	}
}

void comm_request_op::write_cb(void *ctx, async_error_code error_code,
								   size_t bytes)
{
	int error = async_error_to_errno(error_code);
	comm_request_op *self = static_cast<comm_request_op *>(ctx);
	(void)bytes;
	if (error || entry_retired(self->entry_))
		finish(self, CS_STATE_ERROR, error ? error : ECANCELED);
	else if (self->entry_->service)
		complete_server_reply(self);
	else if (start_read(self) < 0)
		finish(self, CS_STATE_ERROR, errno ? errno : EIO);
}

void comm_request_op::read_cb(void *ctx, async_error_code error_code,
								  size_t bytes)
{
	int error = async_error_to_errno(error_code);
	comm_request_op *self = static_cast<comm_request_op *>(ctx);
	(void)bytes;
	if (error || entry_retired(self->entry_))
		finish(self, CS_STATE_ERROR, error ? error : ECANCELED);
	else
	{
		int keep_alive = self->entry_->session->keep_alive_timeout();
		if (park_client_entry(self->entry_, keep_alive))
			self->keep_alive_ = true;
		finish(self, CS_STATE_SUCCESS, 0);
	}
	/* Release the read child's reference (start_read's composed_op_add_ref):
	 * the completion callback and the abort destroy (read_destroy) are
	 * mutually exclusive, mirroring the timed children's child_destroy. */
	composed_op_release(self);
}

void comm_request_op::udp_send_cb(void *ctx, async_error_code error_code,
								   size_t bytes)
{
	int error = async_error_to_errno(error_code);
	comm_request_op *self = static_cast<comm_request_op *>(ctx);
	(void)bytes;
	if (error || entry_retired(self->entry_))
		finish(self, CS_STATE_ERROR, error ? error : ECANCELED);
	else if (self->entry_->service)
		finish(self, CS_STATE_SUCCESS, 0);
	else if (start_udp_read(self) < 0)
		finish(self, CS_STATE_ERROR, errno ? errno : EIO);
}

void comm_request_op::read_destroy(void *ctx)
{
	/* Abort path of the read child (cancelled/closed without completion):
	 * release the reference taken by start_read/start_udp_read.  The child's
	 * exactly-once destroy guard makes this exclusive with read_cb. */
	composed_op_release(static_cast<comm_request_op *>(ctx));
}

void comm_request_op::child_destroy(void *ctx)
{
	composed_op_release(static_cast<comm_request_op *>(ctx));
}

int comm_request_start(CommConnEntry *entry, const struct sockaddr *addr,
					   int addrlen, bool reuse)
{
	if (!entry || !entry->impl || !entry->session || !entry->target)
	{
		errno = EINVAL;
		return -1;
	}
	if (addrlen < 0 || (size_t)addrlen > sizeof(struct sockaddr_storage) ||
		(addrlen != 0 && !addr))
	{
		errno = EINVAL;
		return -1;
	}

	comm_request_op *op = comm_request_op::create(
		entry, comm_request_op::REQUEST_CLIENT, true);
	if (!op)
		return -1;
	if (addrlen != 0)
	{
		memcpy(&op->addr_storage_, addr, (size_t)addrlen);
		op->addr_ = reinterpret_cast<const struct sockaddr *>(
			&op->addr_storage_);
		op->addrlen_ = addrlen;
	}
	op->reuse_ = reuse;

	comm_request_op::start_flow(op);
	composed_op_release(op);
	return 0;
}
