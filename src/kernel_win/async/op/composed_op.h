/*
  ASIO-style composed operation lifetime for the non-template Workflow port.

  This is the C-with-class form of ASIO's handler state.  Each outstanding
  child owns one reference.  A child may select the result, but the final
  handler is not dispatched until every child reference has retired.  This is
  the important timed_cancel_op rule: the winner cancels the other child and
  the user handler runs on the last child completion.

  The user context is released exactly once.  It is released after the final
  handler, or when all child handlers are destroyed without a completion.
*/

#ifndef _ASYNC_OP_COMPOSED_OP_H_
#define _ASYNC_OP_COMPOSED_OP_H_

#include <WinSock2.h>
#include <Windows.h>

#include "../executor.h"
#include "cancellation.h"
#include "../error.h"

class composed_op
{
	public:
	typedef void (*step_type)(composed_op *op, async_error_code error,
							  size_t bytes);

	volatile LONG refs_;
	volatile LONG completed_;
	volatile LONG result_ready_;
	volatile LONG dispatch_started_;
	volatile LONG abandoned_;
	volatile LONG invocations_;
	void (*destroy_)(composed_op *op);
	void (*complete_)(composed_op *op);
	/* ASIO composed implementation: initial call and child continuations. */
	step_type step_;
	executor executor_;
	executor_work_guard work_;
	async_error_code result_error_;
	size_t result_bytes_;
	UINT_PTR result_socket_;
	cancellation_state cancel_state_;
	void (*cancel_)(composed_op *op, cancellation_type type);
	/* User context with an exactly-once destruction hook. */
	void (*ctx_destroy_)(void *ctx);
	void *ctx_;
};

void composed_op_init(composed_op *op, void (*destroy)(composed_op *),
					  void (*complete)(composed_op *));
void composed_op_set_step(composed_op *op, composed_op::step_type step);
void composed_op_start(composed_op *op);
void composed_op_invoke(composed_op *op, async_error_code error, size_t bytes);
void composed_op_complete(composed_op *op, async_error_code error,
						 size_t bytes);
/* Complete the already-settled composed operation on its final handler
 * executor.  Unlike composed_op_complete(), this does not select a result or
 * dispatch through the ASIO executor again. */
void composed_op_complete_handler(composed_op *op);
void composed_op_reset_cancellation(composed_op *op);
cancellation_type composed_op_cancelled(const composed_op *op);
void composed_op_set_executor(composed_op *op, executor ex);
void composed_op_set_cancellation(composed_op *op, const cancellation_slot &slot,
							 void (*cancel)(composed_op *, cancellation_type));
cancellation_slot composed_op_cancellation_slot(composed_op *op);
void composed_op_set_ctx(composed_op *op, void (*destroy)(void *), void *ctx);
void composed_op_add_ref(composed_op *op);
void composed_op_release(composed_op *op);
void composed_op_abandon(composed_op *op);
int composed_op_try_complete(composed_op *op);
	void composed_op_complete_with_error(composed_op *op,
								 async_error_code error,
								 size_t bytes);
int composed_op_dispatch_result(composed_op *op, async_error_code error,
								 size_t bytes);
int composed_op_dispatch(composed_op *op);
void composed_op_cancel(composed_op *op, cancellation_type type);
void composed_op_destroy(void *context);
/* Release a parent reference held by a completed or abandoned child
 * operation.  Unlike composed_op_destroy(), this does not abandon the
 * parent: it is the C equivalent of destroying a child handler that has
 * already been invoked. */
void composed_op_release_context(void *context);

#endif /* _ASYNC_OP_COMPOSED_OP_H_ */
