/*
  AsyncCore: unified handler carrier and dispatch.

  This is the C replacement for asio::handler_work + binder:
  op memory is freed before the handler is dispatched through the executor.
*/

#ifndef _ASYNC_OP_ASYNC_HANDLER_H_
#define _ASYNC_OP_ASYNC_HANDLER_H_

#include <stddef.h>

#include "../executor.h"
#include "../error.h"

class async_handler
{
public:
	void (*callback)(void *context, async_error_code error, size_t bytes);
	void *context;
	/* Called when the executor accepts the handler but later abandons it. */
	void (*destroy)(void *context);
};

class async_handler_call
{
public:
	async_handler handler;
	async_error_code error;
	size_t bytes;
};

class handler_work;

int async_handler_dispatch(executor ex, async_handler handler,
						   async_error_code error, size_t bytes);
int async_handler_dispatch(handler_work &work, async_handler handler,
							   async_error_code error, size_t bytes);

#endif /* _ASYNC_OP_ASYNC_HANDLER_H_ */

