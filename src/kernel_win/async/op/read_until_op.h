/*
 * AsyncCore: fixed-buffer form of asio::async_read_until.
 *
 * The matcher is the non-template equivalent of ASIO's MatchCondition.  It
 * returns > 0 when a boundary was found, 0 when more input is required, and
 * < 0 on failure.  On success it writes the number of bytes through the
 * boundary to *matched.
 */

#ifndef _ASYNC_OP_READ_UNTIL_OP_H_
#define _ASYNC_OP_READ_UNTIL_OP_H_

#include "composed_op.h"
#include "../tcp_socket.h"
#include "../ssl_stream.h"

typedef int (*read_until_match_condition)(void *buffer, size_t size,
									  size_t *matched, void *context);

typedef int (*read_until_submit)(void *stream, void *buffer, size_t size,
									void (*callback)(void *, async_error_code, size_t),
									void *context,
									const cancellation_slot &slot,
									void (*destroy)(void *));

class read_until_op : public composed_op
{
public:
	void *stream_;
	read_until_submit submit_;
	void *buffer_;
	size_t capacity_;
	size_t size_;
	read_until_match_condition match_;
	void *match_context_;
	void (*callback_)(void *, async_error_code, size_t);
	void *context_;
	cancellation_signal child_cancel_;
	bool started_;

	read_until_op()
		: stream_(nullptr), submit_(nullptr), buffer_(nullptr), capacity_(0),
		  size_(0), match_(nullptr), match_context_(nullptr), callback_(nullptr),
		  context_(nullptr), started_(false)
	{
		composed_op_init(this, &read_until_op::destroy,
						 &read_until_op::complete);
		composed_op_set_step(this, &read_until_op::step);
	}

	static void destroy(composed_op *base);
	static void complete(composed_op *base);
	static void cancel(composed_op *base, cancellation_type type);
	static void step(composed_op *base, async_error_code error, size_t bytes);
	static void read_cb(void *context, async_error_code error, size_t bytes);
};

int async_read_until(tcp_socket *socket, void *buffer, size_t capacity,
					 read_until_match_condition match, void *match_context,
					 void (*callback)(void *, async_error_code, size_t), void *context,
					 const cancellation_slot &slot = cancellation_slot(),
					 void (*destroy)(void *) = nullptr);

int async_read_until(ssl_stream *stream, void *buffer, size_t capacity,
					 read_until_match_condition match, void *match_context,
					 void (*callback)(void *, async_error_code, size_t), void *context,
					 const cancellation_slot &slot = cancellation_slot(),
					 void (*destroy)(void *) = nullptr);

#endif /* _ASYNC_OP_READ_UNTIL_OP_H_ */
