/*
  AsyncCore: timed accept composed operation.
*/

#ifndef _ASYNC_OP_TIMED_ACCEPT_OP_H_
#define _ASYNC_OP_TIMED_ACCEPT_OP_H_

#include <WinSock2.h>

#include "composed_op.h"
#include "../tcp_acceptor.h"
#include "../steady_timer.h"

class timed_accept_op : public composed_op
{
	public:
	tcp_acceptor *acceptor_;
	steady_timer *timer_;
	void (*destroy_)(void *);
	void (*callback_)(void *, async_error_code, SOCKET);
	void *context_;
	cancellation_signal io_cancel_;

	timed_accept_op()
		: acceptor_(nullptr), timer_(nullptr), destroy_(nullptr),
		  callback_(nullptr),
		  context_(nullptr)
	{
		composed_op_init(this, &timed_accept_op::destroy,
						 &timed_accept_op::complete);
	}

	static void destroy(composed_op *base);
	static void complete(composed_op *base);
	static void cancel(composed_op *base, cancellation_type type);
	static void destroy_cb(void *ctx);
	static void accept_cb(void *ctx, async_error_code error, SOCKET socket);
	static void timer_cb(void *ctx, async_error_code error);
};

int timed_accept_start(tcp_acceptor *acceptor, steady_timer *timer,
					   int timeout_ms,
					   void (*destroy)(void *),
					   void (*callback)(void *, async_error_code, SOCKET),
					   void *context,
					   cancellation_slot slot = cancellation_slot(),
					   void (*destroy_context)(void *) = nullptr);

#endif /* _ASYNC_OP_TIMED_ACCEPT_OP_H_ */

