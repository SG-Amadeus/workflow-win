/*
  AsyncCore: timed SSL handshake composed operation.
*/

#ifndef _ASYNC_OP_TIMED_HANDSHAKE_OP_H_
#define _ASYNC_OP_TIMED_HANDSHAKE_OP_H_

#include "composed_op.h"
#include "../ssl_stream.h"
#include "../steady_timer.h"

class timed_handshake_op : public composed_op
{
	public:
	ssl_stream *stream_;
	steady_timer *timer_;
	void (*callback_)(void *, async_error_code);
	void *context_;
	cancellation_signal io_cancel_;

	timed_handshake_op()
		: stream_(nullptr), timer_(nullptr), callback_(nullptr),
		  context_(nullptr)
	{
		composed_op_init(this, &timed_handshake_op::destroy,
						 &timed_handshake_op::complete);
	}

	static void destroy(composed_op *base);
	static void complete(composed_op *base);
	static void cancel(composed_op *base, cancellation_type type);
	static void handshake_cb(void *ctx, async_error_code error);
	static void timer_cb(void *ctx, async_error_code error);
};

int timed_handshake_start(ssl_stream *stream, steady_timer *timer,
						  int timeout_ms,
						  void (*callback)(void *, async_error_code), void *context,
						  cancellation_slot slot = cancellation_slot(),
						  void (*destroy_context)(void *) = nullptr);

#endif /* _ASYNC_OP_TIMED_HANDSHAKE_OP_H_ */


