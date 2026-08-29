/*
  AsyncCore: timed UDP send composed operation.
*/

#ifndef _ASYNC_OP_TIMED_UDP_SEND_OP_H_
#define _ASYNC_OP_TIMED_UDP_SEND_OP_H_

#include <WinSock2.h>

#include "composed_op.h"
#include "../steady_timer.h"
#include "../udp_socket.h"

class timed_udp_send_op : public composed_op
{
	public:
	udp_socket *socket_;
	steady_timer *timer_;
	void (*callback_)(void *, async_error_code, size_t);
	void *context_;
	const struct iovec *iov_;
	int iovcnt_;
	const struct sockaddr *addr_;
	int addrlen_;
	cancellation_signal io_cancel_;

	timed_udp_send_op()
		: socket_(nullptr), timer_(nullptr), callback_(nullptr),
		  context_(nullptr), iov_(nullptr), iovcnt_(0),
		  addr_(nullptr), addrlen_(0)
	{
		composed_op_init(this, &timed_udp_send_op::destroy,
						 &timed_udp_send_op::complete);
	}

	static void destroy(composed_op *base);
	static void complete(composed_op *base);
	static void cancel(composed_op *base, cancellation_type type);
	static void send_cb(void *ctx, async_error_code error, size_t bytes);
	static void timer_cb(void *ctx, async_error_code error);
};

int timed_udp_send_start(udp_socket *socket, steady_timer *timer,
						 const struct iovec *iov, int iovcnt,
						 const struct sockaddr *addr, int addrlen,
						 int timeout_ms,
						 void (*callback)(void *, async_error_code, size_t),
						 void *context,
						 cancellation_slot slot = cancellation_slot(),
						 void (*destroy_context)(void *) = nullptr);

#endif /* _ASYNC_OP_TIMED_UDP_SEND_OP_H_ */

