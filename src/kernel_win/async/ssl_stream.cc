#include "ssl_stream.h"
#include "ssl_stream_impl.h"
#include "op/async_handler.h"
#include "op/ssl_op.h"
#include "op/ssl_connect_state.h"
#include "tcp_socket.h"
#include "error.h"

#include <openssl/ssl.h>
#include <openssl/err.h>

#include <errno.h>
#include <new>
#include <stdlib.h>
#include <string.h>
#include <climits>

#include "../list.h"




void ssl_op::retry()
{
	if (::InterlockedExchangeAdd(&cancelled_, 0) != 0 || ::InterlockedCompareExchange(
			const_cast<volatile LONG *>(&impl_->closed_), 0, 0))
	{
		finish(async_system_error(ERROR_OPERATION_ABORTED), 0);
		return;
	}

	async_error_code error;
	int n = 0;
	ssl_want want = SSL_WANT_DONE;
	int out_n = 0;
	bool retry_input = false;

	EnterCriticalSection(&impl_->engine_lock_);
	ERR_clear_error();
	(void)feed_input_locked();
	if (kind_ == 1) /* handshake */
	{
		size_t pending_before = BIO_ctrl_pending(impl_->ext_bio_);
		int r = impl_->server_ ? SSL_accept(impl_->ssl_)
							   : SSL_connect(impl_->ssl_);
			want = engine_want(impl_->ssl_, impl_->ext_bio_, r, pending_before,
							   false, &error, nullptr);
	}
	else if (kind_ == 4) /* shutdown (ASIO do_shutdown calls SSL_shutdown twice) */
	{
		size_t pending_before = BIO_ctrl_pending(impl_->ext_bio_);
		int r = SSL_shutdown(impl_->ssl_);
		if (r == 0)
			r = SSL_shutdown(impl_->ssl_);
			want = engine_want(impl_->ssl_, impl_->ext_bio_, r, pending_before,
							   true, &error, nullptr);
	}
	else if (kind_ == 2) /* read */
	{
		size_t pending_before = BIO_ctrl_pending(impl_->ext_bio_);
		int r = SSL_read(impl_->ssl_, buf_,
						 size_ < INT_MAX ? static_cast<int>(size_) : INT_MAX);
			want = engine_want(impl_->ssl_, impl_->ext_bio_, r, pending_before,
							   false, &error, &n);
		bytes_ = static_cast<size_t>(n);
	}
	else /* write */
	{
		size_t pending_before = BIO_ctrl_pending(impl_->ext_bio_);
		int r = SSL_write(impl_->ssl_, buf_,
						  size_ < INT_MAX ? static_cast<int>(size_) : INT_MAX);
			want = engine_want(impl_->ssl_, impl_->ext_bio_, r, pending_before,
							   false, &error, &n);
		bytes_ = static_cast<size_t>(n);
	}
	if (want == SSL_WANT_READ && impl_->input_size_ != 0)
		retry_input = true;

	if (want == SSL_WANT_WRITE_RETRY || want == SSL_WANT_WRITE_FINAL)
		out_n = BIO_read(impl_->ext_bio_, out_buf_, sizeof out_buf_);
	LeaveCriticalSection(&impl_->engine_lock_);

	want_ = want;
	if (retry_input)
	{
		retry();
		return;
	}

	if (want == SSL_WANT_DONE)
	{
		finish(error, bytes_);
		return;
	}

	if (want == SSL_WANT_READ)
	{
		if (impl_->socket_.async_read_some(in_buf_, sizeof in_buf_,
										   ssl_op::read_cb, this,
										   child_cancel_.slot(),
										   ssl_op::destroy_transport) != 0)
		{
			finish(async_error_from_errno(errno ? errno : EIO), 0);
		}
		return;
	}

	/* SSL_WANT_WRITE_RETRY or SSL_WANT_WRITE_FINAL */
	if (out_n <= 0)
	{
		finish(async_ssl_stream_error(async_ssl_unspecified_system_error), 0);
		return;
	}
	out_len_ = static_cast<size_t>(out_n);
	out_off_ = 0;
	if (impl_->socket_.async_write_some(out_buf_, out_len_,
										ssl_op::write_cb, this,
										child_cancel_.slot(),
										ssl_op::destroy_transport) != 0)
	{
		finish(async_error_from_errno(errno ? errno : EIO), 0);
	}
}

void ssl_op::start()
{
	if (!impl_->ssl_)
	{
		finish(async_ssl_stream_error(async_ssl_unspecified_system_error), 0);
		return;
	}

	EnterCriticalSection(&impl_->mutex_);
	bool control = kind_ == 1 || kind_ == 4;
	bool wait = control
		? impl_->control_current_ || impl_->read_current_ ||
			impl_->write_current_ || !list_empty(&impl_->control_waiting_)
		: impl_->control_current_ || !list_empty(&impl_->control_waiting_) ||
			(kind_ == 2 ? impl_->read_current_ != nullptr
							: impl_->write_current_ != nullptr);
	if (wait)
	{
		list_add_tail(&this->list, waiting_queue(impl_, kind_));
		LeaveCriticalSection(&impl_->mutex_);
		return;
	}

	*current_slot(impl_, kind_) = this;
	LeaveCriticalSection(&impl_->mutex_);
	retry();
}

void ssl_op::run()
{
	retry();
}

void ssl_op::destroy_all()
{
	ssl_stream::impl *impl = impl_;
	struct list_head destroyed;
	INIT_LIST_HEAD(&destroyed);

	EnterCriticalSection(&impl->mutex_);
	ssl_op **current = current_slot(impl, kind_);
	if (*current == this)
		*current = nullptr;
	list_splice_init(waiting_queue(impl, kind_), &destroyed);
	LeaveCriticalSection(&impl->mutex_);

	if (destroy_)
		destroy_(context_);
	impl->release();
	this->~ssl_op();
	free(this);

	while (!list_empty(&destroyed))
	{
		ssl_op *op = list_entry(destroyed.next, ssl_op, list);
		list_del(&op->list);
		if (op->destroy_)
			op->destroy_(op->context_);
		op->impl_->release();
		op->~ssl_op();
		free(op);
	}
}

int ssl_op::feed_input_locked()
{
	while (impl_->input_size_ != 0)
	{
		int n = BIO_write(impl_->ext_bio_, impl_->input_buffer_,
			static_cast<int>(impl_->input_size_));
		if (n <= 0)
			return 1;
		if (static_cast<size_t>(n) < impl_->input_size_)
		{
			memmove(impl_->input_buffer_, impl_->input_buffer_ + n,
				impl_->input_size_ - static_cast<size_t>(n));
		}
		impl_->input_size_ -= static_cast<size_t>(n);
	}
	return 0;
}

void ssl_op::cancel_all(ssl_stream::impl *impl)
{
	if (!impl)
		return;

	struct list_head cancelled;
	INIT_LIST_HEAD(&cancelled);
	EnterCriticalSection(&impl->mutex_);
	ssl_op *active[3] = {
		impl->control_current_, impl->read_current_, impl->write_current_
	};
	struct list_head *queues[3] = {
		&impl->control_waiting_, &impl->read_waiting_, &impl->write_waiting_
	};
	for (int i = 0; i < 3; ++i)
	{
		while (!list_empty(queues[i]))
		{
			ssl_op *op = list_entry(queues[i]->next, ssl_op, list);
			list_del(&op->list);
			::InterlockedExchange(&op->cancelled_, 1);
			list_add_tail(&op->list, &cancelled);
		}
		if (active[i])
			::InterlockedExchange(&active[i]->cancelled_, 1);
	}
	for (int i = 0; i < 3; ++i)
	{
		if (active[i])
			active[i]->child_cancel_.emit(cancellation_type::terminal);
	}
	LeaveCriticalSection(&impl->mutex_);

	while (!list_empty(&cancelled))
	{
		ssl_op *op = list_entry(cancelled.next, ssl_op, list);
		list_del(&op->list);
		op->finish(async_system_error(ERROR_OPERATION_ABORTED), 0);
	}
}

namespace
{


} /* namespace */

ssl_stream::ssl_stream()
	: impl_(nullptr)
{
}

ssl_stream *ssl_stream::create(executor ex, struct ssl_ctx_st *ctx, int server)
{
	void *mem = malloc(sizeof(ssl_stream));
	if (!mem)
	{
		errno = ENOMEM;
		return nullptr;
	}

	ssl_stream *stream = new (mem) ssl_stream();
	if (stream->init(ex, ctx, server) == 0)
		return stream;

	int error = errno;
	stream->~ssl_stream();
	free(stream);
	errno = error;
	return nullptr;
}

void ssl_stream::destroy(ssl_stream *stream)
{
	if (stream)
	{
		stream->~ssl_stream();
		free(stream);
	}
}

int ssl_stream::init(executor ex, struct ssl_ctx_st *ctx, int server)
{
	if (impl_)
	{
		errno = EALREADY;
		return -1;
	}
	if (!ex.context() || !ctx)
	{
		errno = EINVAL;
		return -1;
	}

	void *mem = malloc(sizeof(impl));
	if (mem)
		impl_ = new (mem) impl(ex, ctx, server);
	else
	{
		errno = ENOMEM;
		return -1;
	}

	if (impl_->init_error_)
	{
		int error = impl_->init_error_;
		impl_->release();
		impl_ = nullptr;
		errno = error;
		return -1;
	}
	return 0;
}

ssl_stream::~ssl_stream()
{
	if (impl_)
	{
		this->close();
		impl_->release();
	}
}

int ssl_stream::assign(SOCKET socket)
{
	if (!impl_)
	{
		errno = EINVAL;
		return -1;
	}
	return impl_->socket_.assign(socket);
}

int ssl_stream::set_server(int server)
{
	if (!impl_)
	{
		errno = EINVAL;
		return -1;
	}
	impl_->server_ = server != 0;
	return 0;
}

int ssl_stream::set_init_callback(ssl_stream_init_callback cb, void *userdata)
{
	if (!impl_)
	{
		errno = EINVAL;
		return -1;
	}
	impl_->init_cb_ = cb;
	impl_->init_userdata_ = userdata;
	return 0;
}

int ssl_stream::async_connect(const struct sockaddr *addr, int addrlen,
							  void (*callback)(void *, async_error_code), void *context)
{
	return this->async_connect(addr, addrlen, callback, context, nullptr);
}

int ssl_stream::async_connect(const struct sockaddr *addr, int addrlen,
							  void (*callback)(void *, async_error_code), void *context,
							  void (*destroy)(void *))
{
	if (!impl_ || !callback || !addr || addrlen <= 0)
	{
		errno = EINVAL;
		return -1;
	}

	connect_state *st = (connect_state *)malloc(sizeof *st);
	if (!st)
	{
		errno = ENOMEM;
		return -1;
	}
	st->impl_ = impl_;
	st->callback_ = callback;
	st->context_ = context;
	st->destroy_ = destroy;
	impl_->acquire();

	if (impl_->socket_.async_connect(addr, addrlen,
			&connect_state::connect_cb, st, connect_state::destroy) == 0)
		return 0;

	impl_->release();
	free(st);
	return -1;
}

int ssl_stream::async_connect_transport(const struct sockaddr *addr,
										int addrlen,
										void (*callback)(void *, async_error_code),
										void *context, void (*destroy)(void *))
{
	return this->async_connect_transport(addr, addrlen, callback, context,
									 cancellation_slot(), destroy);
}

int ssl_stream::async_connect_transport(const struct sockaddr *addr,
									int addrlen,
									void (*callback)(void *, async_error_code),
									void *context,
									const cancellation_slot &slot,
									void (*destroy)(void *))
{
	if (!impl_)
	{
		errno = EINVAL;
		return -1;
	}
	return impl_->socket_.async_connect(addr, addrlen, callback, context,
										slot, destroy);
}

int ssl_stream::async_handshake(void (*callback)(void *, async_error_code), void *context)
{
	return this->async_handshake(callback, context, nullptr);
}

int ssl_stream::async_handshake(void (*callback)(void *, async_error_code), void *context,
								void (*destroy)(void *))
{
	return this->async_handshake(callback, context, cancellation_slot(),
								 destroy);
}

int ssl_stream::async_handshake(void (*callback)(void *, async_error_code), void *context,
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


	if (impl_->init_cb_)
	{
		int ret = impl_->init_cb_(impl_->ssl_, impl_->init_userdata_);
		if (ret != 0)
		{
			errno = ret;
			return -1;
		}
	}

	struct bridge
	{
		void (*callback)(void *, async_error_code);
		void *context;
		void (*destroy)(void *);
		static void on_complete(void *ctx, async_error_code error, size_t)
		{
			bridge *b = static_cast<bridge *>(ctx);
			b->callback(b->context, error);
			free(b);
		}
		static void on_destroy(void *ctx)
		{
			bridge *b = static_cast<bridge *>(ctx);
			if (b->destroy)
				b->destroy(b->context);
			free(b);
		}
	};

	bridge *b = (bridge *)malloc(sizeof *b);
	if (!b)
	{
		errno = ENOMEM;
		return -1;
	}
	b->callback = callback;
	b->context = context;
	b->destroy = destroy;

	ssl_op *op = ssl_op::create(impl_, 1, nullptr, 0,
								&bridge::on_complete, b, slot,
								&bridge::on_destroy);
	if (!op)
	{
		free(b);
		errno = ENOMEM;
		return -1;
	}
	op->start();
	return 0;
}

int ssl_stream::async_shutdown(void (*callback)(void *, async_error_code), void *context)
{
	return this->async_shutdown(callback, context, nullptr);
}

int ssl_stream::async_shutdown(void (*callback)(void *, async_error_code), void *context,
							   void (*destroy)(void *))
{
	return this->async_shutdown(callback, context, cancellation_slot(), destroy);
}

int ssl_stream::async_shutdown(void (*callback)(void *, async_error_code), void *context,
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


	struct bridge
	{
		void (*callback)(void *, async_error_code);
		void *context;
		void (*destroy)(void *);
		static void on_complete(void *ctx, async_error_code error, size_t)
		{
			bridge *b = static_cast<bridge *>(ctx);
			b->callback(b->context, error);
			free(b);
		}
		static void on_destroy(void *ctx)
		{
			bridge *b = static_cast<bridge *>(ctx);
			if (b->destroy)
				b->destroy(b->context);
			free(b);
		}
	};

	bridge *b = (bridge *)malloc(sizeof *b);
	if (!b)
	{
		errno = ENOMEM;
		return -1;
	}
	b->callback = callback;
	b->context = context;
	b->destroy = destroy;

	ssl_op *op = ssl_op::create(impl_, 4, nullptr, 0,
								&bridge::on_complete, b, slot,
								&bridge::on_destroy);
	if (!op)
	{
		free(b);
		errno = ENOMEM;
		return -1;
	}
	op->start();
	return 0;
}

int ssl_stream::async_read_some(void *buf, size_t size,
								void (*callback)(void *, async_error_code, size_t),
								void *context)
{
	return this->async_read_some(buf, size, callback, context, nullptr);
}

int ssl_stream::async_read_some(void *buf, size_t size,
								void (*callback)(void *, async_error_code, size_t),
								void *context, void (*destroy)(void *))
{
	return this->async_read_some(buf, size, callback, context,
								 cancellation_slot(), destroy);
}

int ssl_stream::async_read_some(void *buf, size_t size,
								void (*callback)(void *, async_error_code, size_t),
								void *context,
								const cancellation_slot &slot,
								void (*destroy)(void *))
{
	if (!impl_)
	{
		errno = EINVAL;
		return -1;
	}
	if (!callback || !buf || size == 0)
	{
		errno = EINVAL;
		return -1;
	}


	ssl_op *op = ssl_op::create(impl_, 2, buf, size, callback, context,
								slot, destroy);
	if (!op)
	{
		errno = ENOMEM;
		return -1;
	}
	op->start();
	return 0;
}

int ssl_stream::async_write_some(const void *buf, size_t size,
								 void (*callback)(void *, async_error_code, size_t),
								 void *context)
{
	return this->async_write_some(buf, size, callback, context, nullptr);
}

int ssl_stream::async_write_some(const void *buf, size_t size,
								 void (*callback)(void *, async_error_code, size_t),
								 void *context, void (*destroy)(void *))
{
	return this->async_write_some(buf, size, callback, context,
								  cancellation_slot(), destroy);
}

int ssl_stream::async_write_some(const void *buf, size_t size,
								 void (*callback)(void *, async_error_code, size_t),
								 void *context,
								 const cancellation_slot &slot,
								 void (*destroy)(void *))
{
	if (!impl_)
	{
		errno = EINVAL;
		return -1;
	}
	if (!callback || !buf || size == 0)
	{
		errno = EINVAL;
		return -1;
	}


	ssl_op *op = ssl_op::create(impl_, 3,
								const_cast<void *>(buf), size,
								callback, context, slot, destroy);
	if (!op)
	{
		errno = ENOMEM;
		return -1;
	}
	op->start();
	return 0;
}

int ssl_stream::async_writev_some(const struct iovec *iov, int iovcnt,
								  void (*callback)(void *, async_error_code, size_t),
								  void *context)
{
	return this->async_writev_some(iov, iovcnt, callback, context, nullptr);
}

int ssl_stream::async_writev_some(const struct iovec *iov, int iovcnt,
								  void (*callback)(void *, async_error_code, size_t),
								  void *context, void (*destroy)(void *))
{
	return this->async_writev_some(iov, iovcnt, callback, context,
								   cancellation_slot(), destroy);
}

int ssl_stream::async_writev_some(const struct iovec *iov, int iovcnt,
								  void (*callback)(void *, async_error_code, size_t),
								  void *context,
								  const cancellation_slot &slot,
								  void (*destroy)(void *))
{
	if (!impl_)
	{
		errno = EINVAL;
		return -1;
	}
	if (!iov || iovcnt <= 0 || !callback)
	{
		errno = EINVAL;
		return -1;
	}

	size_t total = 0;
	for (int i = 0; i < iovcnt; ++i)
	{
		if (total > (size_t)-1 - iov[i].iov_len)
		{
			errno = EINVAL;
			return -1;
		}
		total += iov[i].iov_len;
	}
	if (total == 0)
	{
		errno = EINVAL;
		return -1;
	}

	/* The buffer must outlive the async operation. */
	struct writev_ctx
	{
		char *data;
		void (*callback)(void *, async_error_code, size_t);
		void *context;
		void (*destroy)(void *);
		static void on_complete(void *arg, async_error_code error, size_t bytes)
		{
			writev_ctx *self = static_cast<writev_ctx *>(arg);
			self->callback(self->context, error, bytes);
			free(self);
		}
		static void on_destroy(void *arg)
		{
			writev_ctx *self = static_cast<writev_ctx *>(arg);
			if (self->destroy)
				self->destroy(self->context);
			free(self);
		}
	};

	if (total > (size_t)-1 - sizeof(writev_ctx))
	{
		errno = EINVAL;
		return -1;
	}
	size_t alloc_size = sizeof(writev_ctx) + total;
	writev_ctx *ctx = (writev_ctx *)malloc(alloc_size);
	if (!ctx)
	{
		errno = ENOMEM;
		return -1;
	}
	ctx->data = (char *)(ctx + 1);
	ctx->callback = callback;
	ctx->context = context;
	ctx->destroy = destroy;

	size_t copied = 0;
	for (int i = 0; i < iovcnt; ++i)
	{
		memcpy(ctx->data + copied, iov[i].iov_base, iov[i].iov_len);
		copied += iov[i].iov_len;
	}

	ssl_op *op = ssl_op::create(impl_, 3, ctx->data, total,
								&writev_ctx::on_complete, ctx, slot,
								&writev_ctx::on_destroy);
	if (!op)
	{
		free(ctx);
		errno = ENOMEM;
		return -1;
	}
	op->start();
	return 0;
}

int ssl_stream::write_transport_some(const void *buf, size_t size)
{
	if (!impl_)
	{
		errno = EINVAL;
		return -1;
	}
	/* SSLWrapper has already called SSL_write() and extracted ciphertext. */
	return impl_->socket_.write_some(buf, size);
}

int ssl_stream::async_wait_read(void (*callback)(void *, async_error_code, size_t),
								void *context, void (*destroy)(void *))
{
	return this->async_wait_read(callback, context, cancellation_slot(),
								destroy);
}

int ssl_stream::async_wait_read(void (*callback)(void *, async_error_code, size_t),
								void *context,
								const cancellation_slot &slot,
								void (*destroy)(void *))
{
	if (!impl_)
	{
		errno = EINVAL;
		return -1;
	}
	return impl_->socket_.async_wait_read(callback, context, slot, destroy);
}

int ssl_stream::cancel()
{
	if (!impl_)
		return 0;
	ssl_op::cancel_all(impl_);
	return impl_->socket_.cancel();
}

int ssl_stream::close()
{
	if (!impl_)
		return 0;
	::InterlockedExchange(&impl_->closed_, 1);
	ssl_op::cancel_all(impl_);
	impl_->socket_.close();
	return 0;
}

SOCKET ssl_stream::native_handle() const
{
	return impl_ ? impl_->socket_.native_handle() : INVALID_SOCKET;
}

executor ssl_stream::get_executor() const
{
	return impl_ ? impl_->executor_ : executor();
}



