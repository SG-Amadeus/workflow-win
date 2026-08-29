#include "comm_service_op.h"
#include "comm_request_op.h"

#include <assert.h>
#include <errno.h>
#include <stdlib.h>

#include <new>

#include "../async/udp_socket.h"
#include "../async/tcp_acceptor.h"
#include "../async/error.h"

comm_service_op::comm_service_op()
	: service_(nullptr), udp_ctx_(nullptr), tcp_ctx_(nullptr), udp_(false),
	  refs_(1), handler_pending_(0), result_error_(0)
{
}

void comm_service_op::acquire()
{
	::InterlockedIncrement(&refs_);
}

void comm_service_op::release()
{
	if (::InterlockedDecrement(&refs_) != 0)
		return;
	destroy(this);
}

void comm_service_op::destroy(comm_service_op *self)
{
	if (self->udp_ctx_)
		self->udp_ctx_->release();
	if (self->tcp_ctx_)
		self->tcp_ctx_->release();
	self->~comm_service_op();
	free(self);
}

int comm_service_op::bind(CommunicatorImpl *impl, CommService *service)
{
	if (!impl || !service || service->listener_handle || service->recv_handle)
	{
		errno = EINVAL;
		return -1;
	}

	if (service->reliable == 0)
	{
		void *mem = malloc(sizeof(UdpServiceContext));
		if (!mem)
		{
			errno = ENOMEM;
			return -1;
		}
		UdpServiceContext *context = new (mem) UdpServiceContext(service);
		if (context->init(&impl->kernel.get_io_context()) != 0)
		{
			int error = errno;
			context->~UdpServiceContext();
			free(context);
			errno = error;
			return -1;
		}

		SOCKET socket = service->create_datagram_socket();
		if (socket == INVALID_SOCKET || context->sock->assign(socket) != 0 ||
			context->sock->bind(service->bind_addr, service->addrlen) != 0)
		{
			int error = errno ? errno : (socket == INVALID_SOCKET ?
				async_win_error_to_errno(::WSAGetLastError()) : EIO);
			if (socket != INVALID_SOCKET)
				::closesocket(socket);
			context->~UdpServiceContext();
			free(context);
			errno = error;
			return -1;
		}

		service->recv_handle = context;
	}
	else
	{
		void *mem = malloc(sizeof(TcpServiceContext));
		if (!mem)
		{
			errno = ENOMEM;
			return -1;
		}
		TcpServiceContext *context = new (mem) TcpServiceContext();
		if (context->init(&impl->kernel.get_io_context()) != 0)
		{
			int error = errno;
			context->~TcpServiceContext();
			free(context);
			errno = error;
			return -1;
		}

		SOCKET socket = service->create_listen_socket();
		if (socket == INVALID_SOCKET || context->acceptor.assign(socket) != 0 ||
			context->acceptor.bind(service->bind_addr, service->addrlen) != 0 ||
			context->acceptor.listen(64) != 0)
		{
			int error = errno ? errno : (socket == INVALID_SOCKET ?
				async_win_error_to_errno(::WSAGetLastError()) : EIO);
			if (socket != INVALID_SOCKET)
				::closesocket(socket);
			context->~TcpServiceContext();
			free(context);
			errno = error;
			return -1;
		}

		service->listener_handle = context;
	}

	service->impl = impl;
	::InterlockedExchange(&service->closing, 0);
	::InterlockedExchange(&service->listener_released, 0);
	service->incref();
	if (!impl->register_service(service))
	{
		unbind(service);
		errno = ECANCELED;
		return -1;
	}
	if (start(service) != 0)
	{
		int error = errno ? errno : EIO;
		unbind(service);
		errno = error;
		return -1;
	}
	return 0;
}

void comm_service_op::unbind(CommService *service)
{
	if (!service)
		return;

	::InterlockedExchange(&service->closing, 1);
	if (service->impl)
		service->impl->unregister_service(service);

	AcquireSRWLockExclusive(&service->lock);
	comm_service_op *op = static_cast<comm_service_op *>(
		service->recv_handle ? service->recv_handle : service->listener_handle);
	service->recv_handle = nullptr;
	service->listener_handle = nullptr;
	ReleaseSRWLockExclusive(&service->lock);
	if (op)
	{
		service->drain(-1);
		if (op->udp_)
			op->udp_ctx_->sock->close();
		else
			op->tcp_ctx_->acceptor.close();
		op->release();
	}
	if (::InterlockedExchange(&service->listener_released, 1) == 0)
		service->decref();
}

int comm_service_op::arm(comm_service_op *self)
{
	CommService *service = self->service_;
	if (::InterlockedCompareExchange(&service->closing, 0, 0))
		return 0;

	if (self->udp_)
	{
		UdpServiceContext *ctx = self->udp_ctx_;
		ctx->fromlen = sizeof ctx->from;
		service->incref();
		ctx->acquire();
		self->acquire();
		int ret = ctx->sock->async_receive_from(ctx->buf, sizeof ctx->buf,
			(struct sockaddr *)&ctx->from, &ctx->fromlen,
			&comm_service_op::udp_recv_cb, self,
			&comm_service_op::udp_recv_destroy);
		if (ret != 0)
		{
			self->release();
			ctx->release();
			service->decref();
			return -1;
		}
		return 0;
	}

	TcpServiceContext *ctx = self->tcp_ctx_;
	void *mem = malloc(sizeof(accept_context));
	if (!mem)
	{
		errno = ENOMEM;
		return -1;
	}
	accept_context *callback = new (mem) accept_context();
	callback->op = self;
	callback->released = 0;
	service->incref();
	self->acquire();
	int ret;
	if (service->listen_timeout > 0)
		ret = timed_accept_start(&ctx->acceptor, &ctx->accept_timer,
			service->listen_timeout, nullptr,
			&comm_service_op::accept_cb, callback, cancellation_slot(),
			&comm_service_op::accept_destroy);
	else
		ret = ctx->acceptor.async_accept(&comm_service_op::accept_cb, callback,
			&comm_service_op::accept_destroy);
	if (ret != 0)
	{
		release_accept_callback(callback);
		return -1;
	}
	return 0;
}

void comm_service_op::accept_cb(void *ctx, async_error_code error_code,
								SOCKET socket)
{
	int error = async_error_to_errno(error_code);
	accept_context *callback = static_cast<accept_context *>(ctx);
	comm_service_op *self = callback->op;
	CommService *service = self->service_;

	if (::InterlockedCompareExchange(&service->closing, 0, 0))
	{
		if (socket != INVALID_SOCKET)
			::closesocket(socket);
		release_accept_callback(callback);
		return;
	}

	if (error || socket == INVALID_SOCKET)
	{
		if (socket != INVALID_SOCKET)
			::closesocket(socket);
		if (error && error != ETIMEDOUT &&
			!::InterlockedCompareExchange(&service->closing, 0, 0))
			comm_service_op::post_completion(self, error);
		if (!::InterlockedCompareExchange(&service->closing, 0, 0))
			arm(self);
		release_accept_callback(callback);
		return;
	}

	struct sockaddr_storage peer;
	int peerlen = sizeof peer;
	if (::getpeername(socket, (struct sockaddr *)&peer, &peerlen) != 0)
	{
		::closesocket(socket);
		arm(self);
		release_accept_callback(callback);
		return;
	}

	CommServiceTarget *target = CommServiceTarget::create(
		service, (struct sockaddr *)&peer, peerlen);
	if (!target)
	{
		::closesocket(socket);
		arm(self);
		release_accept_callback(callback);
		return;
	}

	CommConnEntry *entry = CommConnEntry::create(
		service->impl, &service->impl->kernel.get_io_context(),
		nullptr, nullptr, target, service);
	if (!entry || !service->impl->register_entry(entry))
	{
		target->release();
		if (entry)
			entry->release();
		::closesocket(socket);
		arm(self);
		release_accept_callback(callback);
		return;
	}

	entry->conn = service->new_connection(socket);
	if (!entry->conn)
	{
		target->release();
		entry->release();
		::closesocket(socket);
		arm(self);
		release_accept_callback(callback);
		return;
	}

	if (service->ssl_ctx)
	{
		if (entry->construct_ssl(service->ssl_ctx, 1) != 0 ||
			entry->ssl_sock->assign(socket) != 0)
		{
			::closesocket(socket);
			comm_request_op::destroy_request(entry);
		}
		else
		{
			entry->ssl_sock->set_init_callback(
				&comm_request_op::server_ssl_init, service);
			if (comm_request_op::start_server(entry) != 0)
				comm_request_op::destroy_request(entry);
		}
	}
	else
	{
		if (entry->construct_tcp() != 0 ||
			entry->tcp->assign(socket) != 0)
		{
			::closesocket(socket);
			comm_request_op::destroy_request(entry);
		}
		else
		{
			if (comm_request_op::start_server(entry) != 0)
				comm_request_op::destroy_request(entry);
		}
	}

	arm(self);
	release_accept_callback(callback);
}

void comm_service_op::accept_destroy(void *ctx)
{
	accept_context *callback = static_cast<accept_context *>(ctx);
	release_accept_callback(callback);
}

void comm_service_op::release_accept_callback(accept_context *callback)
{
	if (::InterlockedExchange(&callback->released, 1) == 0)
	{
		comm_service_op *self = callback->op;
		self->service_->decref();
		self->release();
		callback->~accept_context();
		free(callback);
	}
}

void comm_service_op::udp_recv_cb(void *ctx, async_error_code error_code,
								  size_t bytes)
{
	int error = async_error_to_errno(error_code);
	comm_service_op *self = static_cast<comm_service_op *>(ctx);
	UdpServiceContext *uctx = self->udp_ctx_;
	CommService *service = self->service_;

	if (::InterlockedCompareExchange(&service->closing, 0, 0))
	{
		uctx->release();
		service->decref();
		self->release();
		return;
	}

	if (error || bytes == 0)
	{
		if (error && !::InterlockedCompareExchange(&service->closing, 0, 0))
			comm_service_op::post_completion(self, error);
		if (!::InterlockedCompareExchange(&service->closing, 0, 0))
			arm(self);
		uctx->release();
		service->decref();
		self->release();
		return;
	}

	CommServiceTarget *target = CommServiceTarget::create(service,
		(struct sockaddr *)&uctx->from, uctx->fromlen);
	if (!target)
	{
		arm(self);
		uctx->release();
		service->decref();
		self->release();
		return;
	}

	CommConnection *conn = service->new_connection(uctx->sock->native_handle());
	if (!conn)
	{
		target->release();
		arm(self);
		uctx->release();
		service->decref();
		self->release();
		return;
	}

	CommConnEntry *entry = CommConnEntry::create(
		service->impl, &service->impl->kernel.get_io_context(),
		nullptr, conn, target, service);
	if (!entry || !service->impl->register_entry(entry))
	{
		delete conn;
		target->release();
		if (entry)
			entry->release();
		arm(self);
		uctx->release();
		service->decref();
		self->release();
		return;
	}

	entry->udp_sock = uctx->sock;
	entry->udp_shared = true;
	uctx->acquire();
	entry->udp_context = uctx;
	entry->udp_from = uctx->from;
	entry->udp_fromlen = uctx->fromlen;
	if (comm_request_op::start_server_datagram(entry, uctx->buf, bytes) != 0)
		comm_request_op::destroy_request(entry);
	arm(self);
	uctx->release();
	service->decref();
	self->release();
}

void comm_service_op::handle_complete(void *ctx)
{
	comm_service_op *self = static_cast<comm_service_op *>(ctx);
	int error = (int)::InterlockedExchangeAdd(&self->result_error_, 0);
	::InterlockedExchange(&self->handler_pending_, 0);
	self->service_->handle_stop(error);
	self->service_->decref();
	self->release();
}

void comm_service_op::post_completion(comm_service_op *self, int error)
{
	::InterlockedExchange(&self->result_error_, error);
	if (::InterlockedExchange(&self->handler_pending_, 1) != 0)
		return;
	self->service_->incref();
	self->acquire();
	int ret = self->service_->impl->post_handler(
			&comm_service_op::handle_complete, self, &self->handler_task_);
	/* The service keeps the handler pool alive until this operation is drained;
	 * service callbacks never execute on an IOCP worker. */
	assert(ret == 0);
	if (ret != 0)
		RaiseFailFastException(nullptr, nullptr, 0);
}

void comm_service_op::udp_recv_destroy(void *ctx)
{
	comm_service_op *self = static_cast<comm_service_op *>(ctx);
	self->udp_ctx_->release();
	self->service_->decref();
	self->release();
}

int comm_service_op::start(CommService *service)
{
	if (!service || (!service->recv_handle && !service->listener_handle))
	{
		errno = EINVAL;
		return -1;
	}

	void *mem = malloc(sizeof(comm_service_op));
	if (!mem)
	{
		errno = ENOMEM;
		return -1;
	}

	comm_service_op *op = new (mem) comm_service_op();
	op->service_ = service;
	op->udp_ = service->recv_handle != nullptr;
	if (op->udp_)
	{
		op->udp_ctx_ = static_cast<UdpServiceContext *>(service->recv_handle);
		service->recv_handle = op;
	}
	else
	{
		op->tcp_ctx_ = static_cast<TcpServiceContext *>(service->listener_handle);
		service->listener_handle = op;
	}

	if (arm(op) < 0)
	{
		int error = errno;
		if (op->udp_)
			service->recv_handle = nullptr;
		else
			service->listener_handle = nullptr;
		op->release();
		errno = error;
		return -1;
	}

	return 0;
}

