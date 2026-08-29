#ifndef _ASYNC_OP_SSL_OP_H_
#define _ASYNC_OP_SSL_OP_H_

#include "../ssl_stream_impl.h"
#include "cancellation.h"
#include "../list.h"
#include "async_handler.h"

#include <openssl/ssl.h>
#include <openssl/err.h>

#include <new>
#include <stdlib.h>
enum ssl_want
{
	SSL_WANT_DONE = 0,
	SSL_WANT_READ = 1,
	SSL_WANT_WRITE_RETRY = 2,
	SSL_WANT_WRITE_FINAL = 3
};

class ssl_call
{
public:
	void (*callback)(void *, async_error_code, size_t);
	void (*destroy)(void *);
	void *context;
	async_error_code error;
	size_t bytes;
};

static void ssl_call_routine(void *arg)
{
	ssl_call *call = static_cast<ssl_call *>(arg);
	if (call->callback)
		call->callback(call->context, call->error, call->bytes);
	free(call);
}

static void ssl_call_destroy(void *arg)
{
	ssl_call *call = static_cast<ssl_call *>(arg);
	if (call->destroy)
		call->destroy(call->context);
	free(call);
}

static void ssl_call_handler(void *arg, async_error_code, size_t)
{
	ssl_call_routine(arg);
}

static ssl_want engine_want(SSL *ssl, BIO *ext_bio, int result,
							size_t pending_before, bool shutdown,
							async_error_code *error, int *n)
{
	int ssl_error = SSL_get_error(ssl, result);
	int sys_error = static_cast<int>(ERR_get_error());
	size_t pending_after = BIO_ctrl_pending(ext_bio);

	if (n)
		*n = result > 0 ? result : 0;

	if (ssl_error == SSL_ERROR_SSL || ssl_error == SSL_ERROR_SYSCALL)
	{
		*error = sys_error ? async_ssl_error(sys_error) :
			async_ssl_stream_error(async_ssl_unspecified_system_error);
		return pending_after > pending_before ? SSL_WANT_WRITE_FINAL
											  : SSL_WANT_DONE;
	}

	if (ssl_error == SSL_ERROR_WANT_WRITE)
		return SSL_WANT_WRITE_RETRY;

	if (pending_after > pending_before)
		return result > 0 ? SSL_WANT_WRITE_FINAL : SSL_WANT_WRITE_RETRY;

	if (ssl_error == SSL_ERROR_WANT_READ)
		return SSL_WANT_READ;

	if (ssl_error == SSL_ERROR_ZERO_RETURN)
	{
		/* ASIO's shutdown_op translates error::eof to success. */
		*error = shutdown ? async_error_code() :
			async_generic_error(async_error_eof);
		return SSL_WANT_DONE;
	}

	if (ssl_error == SSL_ERROR_NONE || result > 0)
		return SSL_WANT_DONE;

	*error = async_ssl_stream_error(async_ssl_unspecified_system_error);
	return SSL_WANT_DONE;
}

class ssl_op
{
public:
	ssl_stream::impl *impl_;
	struct list_head list;
	int kind_;
	void *buf_;
	size_t size_;
	size_t bytes_;
	void (*callback_)(void *, async_error_code, size_t);
	void *context_;
	void (*destroy_)(void *);
	volatile LONG cancelled_;
	cancellation_state cancel_state_;
	cancellation_signal child_cancel_;
	ssl_want want_;
	size_t out_len_;
	size_t out_off_;
	char in_buf_[17 * 1024];
	char out_buf_[17 * 1024];

	ssl_op(ssl_stream::impl *impl, int kind, void *buf, size_t size,
		   void (*callback)(void *, async_error_code, size_t), void *context,
		   const cancellation_slot &slot, void (*destroy)(void *))
		: impl_(impl), kind_(kind), buf_(buf), size_(size), bytes_(0),
		  callback_(callback), context_(context), destroy_(destroy),
		  cancelled_(0), cancel_state_(slot), want_(SSL_WANT_DONE),
		  out_len_(0), out_off_(0)
	{
		INIT_LIST_HEAD(&list);
		cancel_state_.set_notify(&ssl_op::cancel, this);
		impl_->acquire();
	}

	~ssl_op()
	{
	}

	static ssl_op *create(ssl_stream::impl *impl, int kind,
						  void *buf, size_t size,
						  void (*callback)(void *, async_error_code, size_t),
						  void *context,
						  const cancellation_slot &slot,
						  void (*destroy)(void *) = nullptr)
	{
		void *mem = malloc(sizeof(ssl_op));
		if (!mem)
			return nullptr;
		return new (mem) ssl_op(impl, kind, buf, size, callback, context,
							 slot, destroy);
	}

	static void cancel(void *context, cancellation_type type)
	{
		if (type == cancellation_type::none)
			return;
		ssl_op *self = static_cast<ssl_op *>(context);
		ssl_stream::impl *impl = self->impl_;
		bool queued = false;
		EnterCriticalSection(&impl->mutex_);
		struct list_head *queue = waiting_queue(impl, self->kind_);
		for (struct list_head *node = queue->next;
			node != queue; node = node->next)
		{
			if (node == &self->list)
			{
				list_del(&self->list);
				queued = true;
				break;
			}
		}
		::InterlockedExchange(&self->cancelled_, 1);
		if (!queued)
			self->child_cancel_.emit(type);
		LeaveCriticalSection(&impl->mutex_);
		if (queued)
			self->finish(async_system_error(ERROR_OPERATION_ABORTED), 0);
	}

	void finish(async_error_code error, size_t bytes)
	{
		ssl_op *next[2] = { nullptr, nullptr };
		ssl_stream::impl *impl = impl_;
		async_handler handler = { callback_, context_, destroy_ };
		executor completion_executor = impl->executor_;

		EnterCriticalSection(&impl->mutex_);
		ssl_op **current = current_slot(impl, kind_);
		if (*current == this)
			*current = nullptr;

		/* ASIO's SSL stream has one pending read and one pending write gate.
		 * Handshake and shutdown use both directions and therefore run only
		 * after the two data lanes have become idle. */
		if (!impl->control_current_)
		{
			if (!list_empty(&impl->control_waiting_))
			{
				if (!impl->read_current_ && !impl->write_current_)
				{
					next[0] = list_entry(impl->control_waiting_.next,
						ssl_op, list);
					list_del(&next[0]->list);
					impl->control_current_ = next[0];
				}
			}
			else
			{
				if (!impl->read_current_ &&
						!list_empty(&impl->read_waiting_))
				{
					next[0] = list_entry(impl->read_waiting_.next,
						ssl_op, list);
					list_del(&next[0]->list);
					impl->read_current_ = next[0];
				}
				if (!impl->write_current_ &&
						!list_empty(&impl->write_waiting_))
				{
					next[1] = list_entry(impl->write_waiting_.next,
						ssl_op, list);
					list_del(&next[1]->list);
					impl->write_current_ = next[1];
				}
			}
		}
		LeaveCriticalSection(&impl->mutex_);

		impl->release();
		this->~ssl_op();
		free(this);

		ssl_call *call = (ssl_call *)malloc(sizeof *call);
		if (call)
		{
			call->callback = handler.callback;
			call->destroy = handler.destroy;
			call->context = handler.context;
			call->error = error;
			call->bytes = bytes;

			async_handler dispatch_handler = {
				&ssl_call_handler, call, &ssl_call_destroy
			};
			if (async_handler_dispatch(completion_executor, dispatch_handler,
									   async_error_code(), 0) != 0)
			{
				/* Handler destroyed inside async_handler_dispatch (ASIO). */
			}
		}
		else if (handler.destroy)
			handler.destroy(handler.context);

		if (next[0])
			next[0]->run();
		if (next[1])
			next[1]->run();
	}

	void start();
	void run();
	void retry();
	int feed_input_locked();
	void destroy_all();
	static void cancel_all(ssl_stream::impl *impl);

	static ssl_op **current_slot(ssl_stream::impl *impl, int kind)
	{
		if (kind == 1 || kind == 4)
			return &impl->control_current_;
		if (kind == 2)
			return &impl->read_current_;
		return &impl->write_current_;
	}

	static struct list_head *waiting_queue(ssl_stream::impl *impl, int kind)
	{
		if (kind == 1 || kind == 4)
			return &impl->control_waiting_;
		if (kind == 2)
			return &impl->read_waiting_;
		return &impl->write_waiting_;
	}

	static void destroy_transport(void *arg)
	{
		static_cast<ssl_op *>(arg)->destroy_all();
	}

	static void read_cb(void *arg, async_error_code error, size_t bytes)
	{
		ssl_op *self = static_cast<ssl_op *>(arg);
		if (error)
		{
			self->finish(error, 0);
			return;
		}
		if (bytes == 0)
		{
			self->finish(async_generic_error(async_error_eof), 0);
			return;
		}
		EnterCriticalSection(&self->impl_->engine_lock_);
		int accepted = BIO_write(self->impl_->ext_bio_, self->in_buf_,
			static_cast<int>(bytes));
		if (accepted < 0)
			accepted = 0;
		if (accepted > 0 && static_cast<size_t>(accepted) < bytes)
		{
			size_t remaining = bytes - static_cast<size_t>(accepted);
			if (remaining > sizeof self->impl_->input_buffer_ ||
				self->impl_->input_size_ != 0)
			{
				LeaveCriticalSection(&self->impl_->engine_lock_);
				self->finish(async_ssl_stream_error(
					async_ssl_unspecified_system_error), 0);
				return;
			}
			memcpy(self->impl_->input_buffer_, self->in_buf_ + accepted,
				remaining);
			self->impl_->input_size_ = remaining;
		}
		LeaveCriticalSection(&self->impl_->engine_lock_);
		if (static_cast<size_t>(accepted) > bytes)
		{
			self->finish(async_ssl_stream_error(
				async_ssl_unspecified_system_error), 0);
			return;
		}
		self->retry();
	}

	static void write_cb(void *arg, async_error_code error, size_t bytes)
	{
		ssl_op *self = static_cast<ssl_op *>(arg);
		if (error)
		{
			self->finish(error, 0);
			return;
		}

		size_t done = self->out_off_ + bytes;
		if (bytes == 0 || done > self->out_len_)
		{
			self->finish(async_ssl_stream_error(
				async_ssl_unspecified_system_error), 0);
			return;
		}

		if (done < self->out_len_)
		{
			self->out_off_ = done;
			if (self->impl_->socket_.async_write_some(
					self->out_buf_ + done, self->out_len_ - done,
					ssl_op::write_cb, self, self->child_cancel_.slot(),
					ssl_op::destroy_transport) != 0)
			{
				self->finish(async_error_from_errno(errno ? errno : EIO), 0);
			}
			return;
		}

		self->out_off_ = done;
		if (self->want_ == SSL_WANT_WRITE_FINAL)
			self->finish(async_error_code(), self->bytes_);
		else
			self->retry();
	}
};

#endif /* _ASYNC_OP_SSL_OP_H_ */


