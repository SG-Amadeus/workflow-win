/*
  AsyncCore: accept operation for tcp_acceptor.
*/

#ifndef _ASYNC_OP_ACCEPT_OP_H_
#define _ASYNC_OP_ACCEPT_OP_H_

#include "win_iocp_operation.h"
#include "../service/handler_work.h"
#include "../service/cancel_token.h"

class op_pools;

class accept_op : public win_iocp_operation
{
public:
	op_pools *pools_;
	handler_work work_;
	SOCKET listener_;
	SOCKET accepted_;
	unsigned char output_buffer_[(sizeof(sockaddr_storage) + 16) * 2];
	cancel_token *cancel_token_;
	void (*callback_)(void *, async_error_code, SOCKET);
	void *context_;
	void (*destroy_)(void *);
	volatile LONG cancelled_;

	accept_op()
		: win_iocp_operation(&accept_op::do_complete),
		  listener_(INVALID_SOCKET), accepted_(INVALID_SOCKET),
		  cancel_token_(nullptr), cancelled_(0)
	{
	}
	~accept_op()
	{
		if (accepted_ != INVALID_SOCKET)
			::closesocket(accepted_);
	}

	static void do_complete(void *owner, win_iocp_operation *base,
							async_error_code error, size_t bytes);
};

accept_op *accept_op_alloc(op_pools *pools);
void accept_op_free(accept_op *op);

#endif /* _ASYNC_OP_ACCEPT_OP_H_ */

