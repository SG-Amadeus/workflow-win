#include "async_handler.h"

#include "../service/handler_work.h"
#include <stdlib.h>

namespace
{

void async_handler_call_routine(void *arg)
{
	async_handler_call *call = static_cast<async_handler_call *>(arg);
	call->handler.callback(call->handler.context, call->error, call->bytes);
	free(call);
}

void async_handler_call_destroy(void *arg)
{
	async_handler_call *call = static_cast<async_handler_call *>(arg);
	if (call->handler.destroy)
		call->handler.destroy(call->handler.context);
	free(call);
}

} /* namespace */

int async_handler_dispatch(handler_work &work, async_handler handler,
						   async_error_code error, size_t bytes)
{
	void *mem = malloc(sizeof(async_handler_call));
	if (!mem)
	{
		/* The handler can never be invoked: destroy it exactly once (ASIO
		 * handler-object semantics; dispatch failure == handler destruction). */
		if (handler.destroy)
			handler.destroy(handler.context);
		return -1;
	}

	async_handler_call *call = static_cast<async_handler_call *>(mem);
	call->handler = handler;
	call->error = error;
	call->bytes = bytes;

	if (work.complete(&async_handler_call_routine, call,
					  &async_handler_call_destroy) != 0)
	{
		free(call);
		if (handler.destroy)
			handler.destroy(handler.context);
		return -1;
	}

	return 0;
}

int async_handler_dispatch(executor ex, async_handler handler,
						   async_error_code error, size_t bytes)
{
	handler_work work(ex);
	return async_handler_dispatch(work, handler, error, bytes);
}

