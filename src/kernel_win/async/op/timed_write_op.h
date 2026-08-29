/*
  AsyncCore: timed write composed operation for tcp_socket.
*/

#ifndef _ASYNC_OP_TIMED_WRITE_OP_H_
#define _ASYNC_OP_TIMED_WRITE_OP_H_

#include <WinSock2.h>

#include "composed_op.h"
#include "write_op.h"
#include "../steady_timer.h"
#include "../tcp_socket.h"

class timed_write_op : public composed_op
{
	public:
	tcp_socket *socket_;
	steady_timer *timer_;
	void (*callback_)(void *, async_error_code, size_t);
	void *context_;
	cancellation_signal io_cancel_;

	timed_write_op()
		: socket_(nullptr), timer_(nullptr), callback_(nullptr),
			context_(nullptr)
	{
		composed_op_init(this, &timed_write_op::destroy,
						 &timed_write_op::complete);
	}

	static void destroy(composed_op *base);
	static void complete(composed_op *base);
	static void cancel(composed_op *base, cancellation_type type);
	static void write_cb(void *ctx, async_error_code error, size_t bytes);
	static void timer_cb(void *ctx, async_error_code error);
};

int timed_write_start(tcp_socket *socket, steady_timer *timer,
					  const struct iovec *iov, int iovcnt,
					  int timeout_ms,
						  void (*callback)(void *, async_error_code, size_t),
						  void *context,
						  cancellation_slot slot = cancellation_slot(),
						  void (*destroy_context)(void *) = nullptr);

#endif /* _ASYNC_OP_TIMED_WRITE_OP_H_ */

