/*
 * AsyncCore: fixed iovec form of asio::async_write.
 *
 * The operation owns the copied buffer descriptors and repeatedly starts the
 * stream's async_writev_some operation until all buffers are consumed.
 */

#ifndef _ASYNC_OP_WRITE_OP_H_
#define _ASYNC_OP_WRITE_OP_H_

#include "composed_op.h"
#include "../tcp_socket.h"
#include "../ssl_stream.h"

typedef int (*write_submit)(void *stream, const struct iovec *iov,
							 int iovcnt, void (*callback)(void *, async_error_code, size_t),
							 void *context, const cancellation_slot &slot,
							 void (*destroy)(void *));

class write_op : public composed_op
{
	public:
	void *stream_;
	write_submit submit_;
	struct iovec *iov_;
	int iovcnt_;
	int iov_index_;
	size_t total_bytes_;
	void (*callback_)(void *, async_error_code, size_t);
	void *context_;
	cancellation_signal child_cancel_;
	bool started_;

	write_op()
		: stream_(nullptr), submit_(nullptr), iov_(nullptr), iovcnt_(0),
		  iov_index_(0), total_bytes_(0), callback_(nullptr), context_(nullptr),
		  started_(false)
	{
		composed_op_init(this, &write_op::destroy, &write_op::complete);
		composed_op_set_step(this, &write_op::step);
	}

	static void destroy(composed_op *base);
	static void complete(composed_op *base);
	static void cancel(composed_op *base, cancellation_type type);
	static void step(composed_op *base, async_error_code error, size_t bytes);
	static void write_cb(void *context, async_error_code error, size_t bytes);
};

int async_write(tcp_socket *socket, const struct iovec *iov, int iovcnt,
				 void (*callback)(void *, async_error_code, size_t), void *context,
				 const cancellation_slot &slot = cancellation_slot(),
				 void (*destroy)(void *) = nullptr);

int async_write(ssl_stream *stream, const struct iovec *iov, int iovcnt,
				 void (*callback)(void *, async_error_code, size_t), void *context,
				 const cancellation_slot &slot = cancellation_slot(),
				 void (*destroy)(void *) = nullptr);

#endif /* _ASYNC_OP_WRITE_OP_H_ */
