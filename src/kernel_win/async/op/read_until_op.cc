#include "read_until_op.h"

#include <errno.h>
#include <new>
#include <stdlib.h>

namespace
{

int tcp_read_until_submit(void *stream, void *buffer, size_t size,
						  void (*callback)(void *, async_error_code, size_t), void *context,
						  const cancellation_slot &slot, void (*destroy)(void *))
{
	return static_cast<tcp_socket *>(stream)->async_read_some(
		buffer, size, callback, context, slot, destroy);
}

int ssl_read_until_submit(void *stream, void *buffer, size_t size,
						  void (*callback)(void *, async_error_code, size_t), void *context,
						  const cancellation_slot &slot, void (*destroy)(void *))
{
	return static_cast<ssl_stream *>(stream)->async_read_some(
		buffer, size, callback, context, slot, destroy);
}

int start_read_until(read_until_op *self)
{
	if (self->size_ == self->capacity_)
	{
		composed_op_complete(self, async_error_from_errno(EMSGSIZE), 0);
		return -1;
	}

	composed_op_add_ref(self);
	int ret = self->submit_(self->stream_,
		static_cast<char *>(self->buffer_) + self->size_,
		self->capacity_ - self->size_, &read_until_op::read_cb, self,
		self->child_cancel_.slot(), composed_op_destroy);
	if (ret != 0)
	{
		async_error_code error = async_error_from_errno(errno ? errno : EIO);
		composed_op_release(self);
		composed_op_complete(self, error, 0);
		return -1;
	}
	return 0;
}

} /* namespace */

void read_until_op::destroy(composed_op *base)
{
	read_until_op *self = static_cast<read_until_op *>(base);
	self->~read_until_op();
	free(self);
}

void read_until_op::complete(composed_op *base)
{
	read_until_op *self = static_cast<read_until_op *>(base);
	self->callback_(self->context_, self->result_error_, self->result_bytes_);
}

void read_until_op::cancel(composed_op *base, cancellation_type type)
{
	read_until_op *self = static_cast<read_until_op *>(base);
	if (type != cancellation_type::none)
		self->child_cancel_.emit(type);
}

void read_until_op::step(composed_op *base, async_error_code error, size_t bytes)
{
	read_until_op *self = static_cast<read_until_op *>(base);
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
	}
	else
	{
		if (bytes > self->capacity_ - self->size_)
		{
		composed_op_complete(self, async_error_from_errno(EIO), 0);
			return;
		}
		self->size_ += bytes;
		if (error)
		{
			composed_op_complete(self, error, 0);
			return;
		}
		if (bytes == 0)
		{
			/* ASIO stops a read-until continuation on a successful zero-byte
			 * read and reports the bytes already inspected. */
			composed_op_complete(self, async_error_code(), self->size_);
			return;
		}
	}

	size_t matched = 0;
	int ret = self->match_(self->buffer_, self->size_, &matched,
		self->match_context_);
	if (ret > 0)
	{
		if (matched > self->size_)
		{
			composed_op_complete(self, async_error_from_errno(EINVAL), 0);
			return;
		}
		composed_op_complete(self, async_error_code(), matched);
		return;
	}
	if (ret < 0)
	{
		composed_op_complete(self,
			async_error_from_errno(errno ? errno : EIO), 0);
		return;
	}
	if (self->size_ == self->capacity_)
	{
		/* This is the fixed-buffer equivalent of asio::error::not_found. */
		composed_op_complete(self,
			async_generic_error(async_error_not_found), 0);
		return;
	}
	if (composed_op_cancelled(self) != cancellation_type::none)
	{
		composed_op_complete(self,
			async_system_error(ERROR_OPERATION_ABORTED), 0);
		return;
	}
	(void)start_read_until(self);
}

void read_until_op::read_cb(void *context, async_error_code error, size_t bytes)
{
	read_until_op *self = static_cast<read_until_op *>(context);
	composed_op_invoke(self, error, bytes);
	composed_op_release(self);
}

static int async_read_until_start(void *stream, executor ex,
							  read_until_submit submit, void *buffer,
							  size_t capacity,
							  read_until_match_condition match,
							  void *match_context,
							  void (*callback)(void *, async_error_code, size_t),
							  void *context, const cancellation_slot &slot,
							  void (*destroy)(void *))
{
	if (!stream || !submit || !buffer || capacity == 0 || !match || !callback)
	{
		errno = EINVAL;
		return -1;
	}

	void *mem = malloc(sizeof(read_until_op));
	if (!mem)
	{
		errno = ENOMEM;
		return -1;
	}

	read_until_op *op = new (mem) read_until_op();
	op->stream_ = stream;
	op->submit_ = submit;
	op->buffer_ = buffer;
	op->capacity_ = capacity;
	op->match_ = match;
	op->match_context_ = match_context;
	op->callback_ = callback;
	op->context_ = context;
	composed_op_set_ctx(op, destroy, context);
	composed_op_set_executor(op, ex);
	composed_op_set_cancellation(op, slot, &read_until_op::cancel);
	composed_op_start(op);
	composed_op_release(op);
	return 0;
}

int async_read_until(tcp_socket *socket, void *buffer, size_t capacity,
					 read_until_match_condition match, void *match_context,
					 void (*callback)(void *, async_error_code, size_t), void *context,
					 const cancellation_slot &slot, void (*destroy)(void *))
{
	return async_read_until_start(socket,
		socket ? socket->get_executor() : executor(),
		tcp_read_until_submit, buffer, capacity, match, match_context,
		callback, context, slot, destroy);
}

int async_read_until(ssl_stream *stream, void *buffer, size_t capacity,
					 read_until_match_condition match, void *match_context,
					 void (*callback)(void *, async_error_code, size_t), void *context,
					 const cancellation_slot &slot, void (*destroy)(void *))
{
	return async_read_until_start(stream,
		stream ? stream->get_executor() : executor(),
		ssl_read_until_submit, buffer, capacity, match, match_context,
		callback, context, slot, destroy);
}
