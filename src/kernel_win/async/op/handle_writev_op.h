/*
  AsyncCore: vector write operation for random_access_handle.
*/

#ifndef _ASYNC_OP_HANDLE_WRITEV_OP_H_
#define _ASYNC_OP_HANDLE_WRITEV_OP_H_

#include "win_iocp_operation.h"
#include "../service/handler_work.h"
#include "../../../PlatformSocket.h"

#include <stdlib.h>

class op_pools;

class handle_writev_op : public win_iocp_operation
{
public:
	op_pools *pools_;
	handler_work work_;
	void (*callback_)(void *, async_error_code, size_t);
	void *context_;
	void (*destroy_)(void *);
	struct iovec *iov_;
	int iov_count_;
	char *temp_;
	size_t temp_size_;

	handle_writev_op()
		: win_iocp_operation(&handle_writev_op::do_complete),
		  iov_(nullptr), iov_count_(0), temp_(nullptr), temp_size_(0)
	{
	}
	~handle_writev_op()
	{
		free(iov_);
		free(temp_);
	}

	static void do_complete(void *owner, win_iocp_operation *base,
							async_error_code error, size_t bytes);
};

handle_writev_op *handle_writev_op_alloc(op_pools *pools);
void handle_writev_op_free(handle_writev_op *op);

#endif /* _ASYNC_OP_HANDLE_WRITEV_OP_H_ */

