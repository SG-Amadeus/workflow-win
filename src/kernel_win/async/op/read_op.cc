#include "read_op.h"

#include <errno.h>
#include <new>
#include <stdlib.h>
#include <string.h>

namespace
{

int tcp_read_submit(void *stream, const struct iovec *iov, int iovcnt,
					void (*callback)(void *, async_error_code, size_t), void *context,
					const cancellation_slot &slot, void (*destroy)(void *))
{
	return static_cast<tcp_socket *>(stream)->async_readv_some(
		iov, iovcnt, callback, context, slot, destroy);
}

int ssl_read_submit(void *stream, const struct iovec *iov, int iovcnt,
					void (*callback)(void *, async_error_code, size_t), void *context,
					const cancellation_slot &slot, void (*destroy)(void *))
{
	/* ssl_stream exposes one mutable buffer at a time.  The composed read
	 * advances to the next iovec after this child completes. */
	if (iovcnt <= 0)
	{
		errno = EINVAL;
		return -1;
	}
	return static_cast<ssl_stream *>(stream)->async_read_some(
		iov[0].iov_base, iov[0].iov_len, callback, context, slot, destroy);
}

int skip_empty(const read_op *self)
{
	int index = self->iov_index_;
	while (index < self->iovcnt_ && self->iov_[index].iov_len == 0)
		++index;
	return index;
}

int start_read(read_op *self)
{
	composed_op_add_ref(self);
	int ret = self->submit_(self->stream_, self->iov_ + self->iov_index_,
		self->iovcnt_ - self->iov_index_, &read_op::read_cb, self,
		self->child_cancel_.slot(), composed_op_destroy);
	if (ret != 0)
	{
		async_error_code error = async_error_from_errno(errno ? errno : EIO);
		composed_op_release(self);
		composed_op_complete(self, error, self->total_bytes_);
		return -1;
	}
	return 0;
}

} /* namespace */

void read_op::destroy(composed_op *base)
{
	read_op *self = static_cast<read_op *>(base);
	free(self->iov_);
	self->~read_op();
	free(self);
}

void read_op::complete(composed_op *base)
{
	read_op *self = static_cast<read_op *>(base);
	self->callback_(self->context_, self->result_error_, self->result_bytes_);
}

void read_op::cancel(composed_op *base, cancellation_type type)
{
	read_op *self = static_cast<read_op *>(base);
	if (type != cancellation_type::none)
		self->child_cancel_.emit(type);
}

void read_op::step(composed_op *base, async_error_code error, size_t bytes)
{
	read_op *self = static_cast<read_op *>(base);
	if (::InterlockedCompareExchange(&self->completed_, 0, 0) != 0)
		return;

	if (!self->started_)
	{
		self->started_ = true;
		if (composed_op_cancelled(self) != cancellation_type::none)
		{
			composed_op_complete(self,
				async_system_error(ERROR_OPERATION_ABORTED), 0);
			return;
		}
		if (self->iov_index_ == self->iovcnt_)
		{
			composed_op_complete(self, async_error_code(), 0);
			return;
		}
		(void)start_read(self);
		return;
	}

	size_t remaining = bytes;
	while (remaining && self->iov_index_ < self->iovcnt_)
	{
		struct iovec *iov = &self->iov_[self->iov_index_];
		if (remaining < iov->iov_len)
		{
			iov->iov_base = static_cast<char *>(iov->iov_base) + remaining;
			iov->iov_len -= remaining;
			remaining = 0;
		}
		else
		{
			remaining -= iov->iov_len;
			iov->iov_len = 0;
			++self->iov_index_;
		}
	}

	if (remaining != 0)
	{
		composed_op_complete(self, async_error_from_errno(EIO), self->total_bytes_);
		return;
	}

	self->total_bytes_ += bytes;
	if (error)
	{
		composed_op_complete(self, error, self->total_bytes_);
		return;
	}

	if (bytes == 0)
	{
		/* This is the transfer_all completion condition used by ASIO. */
		composed_op_complete(self, async_error_code(), self->total_bytes_);
		return;
	}

	self->iov_index_ = skip_empty(self);
	if (self->iov_index_ == self->iovcnt_)
	{
		composed_op_complete(self, async_error_code(), self->total_bytes_);
		return;
	}

	if (composed_op_cancelled(self) != cancellation_type::none)
	{
		composed_op_complete(self,
			async_system_error(ERROR_OPERATION_ABORTED), self->total_bytes_);
		return;
	}
	(void)start_read(self);
}

void read_op::read_cb(void *context, async_error_code error, size_t bytes)
{
	read_op *self = static_cast<read_op *>(context);
	composed_op_invoke(self, error, bytes);
	composed_op_release(self);
}

static int async_read_start(void *stream, executor ex, read_submit submit,
							const struct iovec *iov, int iovcnt,
							void (*callback)(void *, async_error_code, size_t), void *context,
							const cancellation_slot &slot, void (*destroy)(void *))
{
	if (!stream || !submit || !iov || iovcnt <= 0 || !callback)
	{
		errno = EINVAL;
		return -1;
	}

	void *mem = malloc(sizeof(read_op));
	if (!mem)
	{
		errno = ENOMEM;
		return -1;
	}

	read_op *op = new (mem) read_op();
	op->stream_ = stream;
	op->submit_ = submit;
	op->iovcnt_ = iovcnt;
	op->callback_ = callback;
	op->context_ = context;
	op->iov_ = static_cast<struct iovec *>(
		malloc((size_t)iovcnt * sizeof(struct iovec)));
	if (!op->iov_)
	{
		composed_op_release(op);
		errno = ENOMEM;
		return -1;
	}

	memcpy(op->iov_, iov, (size_t)iovcnt * sizeof(struct iovec));
	op->iov_index_ = skip_empty(op);
	composed_op_set_ctx(op, destroy, context);
	composed_op_set_executor(op, ex);
	composed_op_set_cancellation(op, slot, &read_op::cancel);

	composed_op_start(op);
	composed_op_release(op);
	return 0;
}

int async_read(tcp_socket *socket, const struct iovec *iov, int iovcnt,
			   void (*callback)(void *, async_error_code, size_t), void *context,
			   const cancellation_slot &slot, void (*destroy)(void *))
{
	return async_read_start(socket,
		socket ? socket->get_executor() : executor(), tcp_read_submit,
		iov, iovcnt, callback, context, slot, destroy);
}

int async_read(ssl_stream *stream, const struct iovec *iov, int iovcnt,
			   void (*callback)(void *, async_error_code, size_t), void *context,
			   const cancellation_slot &slot, void (*destroy)(void *))
{
	return async_read_start(stream,
		stream ? stream->get_executor() : executor(), ssl_read_submit,
		iov, iovcnt, callback, context, slot, destroy);
}
