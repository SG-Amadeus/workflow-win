/*
  AsyncCore: timed connect composed operation.

  This is a C-style composed operation: it coordinates tcp_socket::async_connect
  and steady_timer::async_wait, delivering exactly one callback.
*/

#ifndef _ASYNC_OP_TIMED_CONNECT_OP_H_
#define _ASYNC_OP_TIMED_CONNECT_OP_H_

#include <WinSock2.h>

#include "composed_op.h"
#include "cancellation.h"
#include "../steady_timer.h"
#include "../tcp_socket.h"

class timed_connect_op : public composed_op
{
	public:
	tcp_socket *socket_;
	steady_timer *timer_;
	void (*callback_)(void *, async_error_code);
	void *context_;
	cancellation_signal io_cancel_;

	timed_connect_op()
		: socket_(nullptr), timer_(nullptr), callback_(nullptr),
		  context_(nullptr)
	{
		composed_op_init(this, &timed_connect_op::destroy,
						 &timed_connect_op::complete);
	}

	static void destroy(composed_op *base);
	static void complete(composed_op *base);
	static void cancel(composed_op *base, cancellation_type type);
	static void connect_cb(void *ctx, async_error_code error);
	static void timer_cb(void *ctx, async_error_code error);
};

static_assert(offsetof(timed_connect_op, socket_) >= sizeof(composed_op),
			  "timed_connect_op fields must follow composed_op");

int timed_connect_start(tcp_socket *socket, steady_timer *timer,
						const struct sockaddr *addr, int addrlen,
						int timeout_ms,
		void (*callback)(void *, async_error_code), void *context,
		cancellation_slot slot = cancellation_slot(),
		void (*destroy_context)(void *) = nullptr);

#endif /* _ASYNC_OP_TIMED_CONNECT_OP_H_ */

