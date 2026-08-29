/*
  AsyncCore: legacy SSL connect composed state used by Runtime.
*/

#ifndef _ASYNC_OP_SSL_CONNECT_STATE_H_
#define _ASYNC_OP_SSL_CONNECT_STATE_H_

#include "../ssl_stream_impl.h"
#include "ssl_op.h"
#include "async_handler.h"

#include <stdlib.h>

class connect_state
{
public:
	ssl_stream::impl *impl_;
	void (*callback_)(void *, async_error_code);
	void *context_;
	void (*destroy_)(void *);

	static void finish(connect_state *self)
	{
		self->impl_->release();
		free(self);
	}

	static void destroy(void *arg)
	{
		connect_state *self = static_cast<connect_state *>(arg);
		if (self->destroy_)
			self->destroy_(self->context_);
		finish(self);
	}

	static void ssl_call_cb(void *ctx, async_error_code error, size_t)
	{
		connect_state *self = static_cast<connect_state *>(ctx);
		self->callback_(self->context_, error);
		finish(self);
	}

	static void connect_cb(void *arg, async_error_code error)
	{
		connect_state *self = static_cast<connect_state *>(arg);
		if (error)
		{
			ssl_call *call = (ssl_call *)malloc(sizeof *call);
			if (call)
			{
				call->callback = &connect_state::ssl_call_cb;
				call->destroy = &connect_state::destroy;
				call->context = self;
				call->error = error;
				call->bytes = 0;

				async_handler handler = {
					&ssl_call_handler, call, &ssl_call_destroy
				};
				if (async_handler_dispatch(self->impl_->executor_, handler,
									   async_error_code(), 0) == 0)
					return;
				/* Handler destroyed inside async_handler_dispatch (ASIO). */
				return;
			}
			/* Cannot defer the handler: destroy it exactly once (ASIO
			 * handler-object semantics). */
			connect_state::destroy(self);
			return;
		}

		ssl_op *op = ssl_op::create(self->impl_, 1, nullptr, 0,
			&connect_state::ssl_call_cb, self, cancellation_slot(),
			&connect_state::destroy);
		if (!op)
		{
			self->callback_(self->context_,
				async_error_from_errno(ENOMEM));
			finish(self);
			return;
		}
		op->start();
	}
};

#endif /* _ASYNC_OP_SSL_CONNECT_STATE_H_ */
