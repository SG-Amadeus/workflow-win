/*
  AsyncCore: base operation used by the timer queue.
*/

#ifndef _ASYNC_OP_TIMER_OP_H_
#define _ASYNC_OP_TIMER_OP_H_

#include "win_iocp_operation.h"

class wait_op : public win_iocp_operation
{
public:
	async_error_code error;
	void *cancellation_key;

	explicit wait_op(func_type func)
		: win_iocp_operation(func), error(), cancellation_key(nullptr) {}
};

#endif /* _ASYNC_OP_TIMER_OP_H_ */
