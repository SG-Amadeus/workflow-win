/*
  AsyncCore: timer wait operation for steady_timer.
*/

#ifndef _ASYNC_OP_TIMER_WAIT_OP_H_
#define _ASYNC_OP_TIMER_WAIT_OP_H_

#include "win_iocp_operation.h"
#include "timer_op.h"
#include "../steady_timer.h"
#include "../service/handler_work.h"

class op_pools;

class steady_timer_wait_op : public wait_op
{
public:
	steady_timer::impl *impl_;
	handler_work work_;
	void (*callback_)(void *, async_error_code);
	void *context_;
	void (*destroy_)(void *);
	op_pools *pools_;

	steady_timer_wait_op()
		: wait_op(&steady_timer_wait_op::do_complete),
		  impl_(nullptr), pools_(nullptr) {}

	static void do_complete(void *owner, win_iocp_operation *base,
							async_error_code error, size_t bytes);
};

steady_timer_wait_op *timer_wait_op_alloc(op_pools *pools);
void timer_wait_op_free(steady_timer_wait_op *op);

#endif /* _ASYNC_OP_TIMER_WAIT_OP_H_ */

