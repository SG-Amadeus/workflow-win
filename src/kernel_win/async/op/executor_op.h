/*
  AsyncCore: executor post/dispatch/defer operation.
*/

#ifndef _ASYNC_OP_EXECUTOR_OP_H_
#define _ASYNC_OP_EXECUTOR_OP_H_

#include "win_iocp_operation.h"

class op_pools;

class executor_op : public win_iocp_operation
{
	public:
	void (*routine_)(void *);
	void (*destroy_)(void *);
	void *context_;
	op_pools *pools_;

	executor_op(void (*routine)(void *), void *context,
				 void (*destroy)(void *), op_pools *pools)
		: win_iocp_operation(&executor_op::do_complete),
		  routine_(routine), destroy_(destroy), context_(context), pools_(pools)
	{
	}

	static void do_complete(void *owner, win_iocp_operation *base,
							async_error_code error, size_t bytes);
};

#endif /* _ASYNC_OP_EXECUTOR_OP_H_ */

