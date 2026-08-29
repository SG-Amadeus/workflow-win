/*
 * Non-template port of ASIO's iocp_op_cancellation.
 *
 * The wrapper is the OVERLAPPED submitted to Windows.  Its target is the
 * concrete operation that owns the handler and handler_work.
 */

#ifndef _ASYNC_OP_IOCP_OP_CANCELLATION_H_
#define _ASYNC_OP_IOCP_OP_CANCELLATION_H_

#include "cancellation.h"
#include "win_iocp_operation.h"

class iocp_op_cancellation : public win_iocp_operation
{
public:
	static iocp_op_cancellation *create(HANDLE handle,
										   win_iocp_operation *target,
										   const cancellation_slot &slot,
										   volatile LONG *cancel_requested = nullptr);

	static void destroy(iocp_op_cancellation *op);

private:
	iocp_op_cancellation(HANDLE handle, win_iocp_operation *target,
						 const cancellation_slot &slot,
						 volatile LONG *cancel_requested);
	~iocp_op_cancellation();

	static void do_cancel(void *context, cancellation_type type);
	static void acquire(void *context);
	static void release(void *context);
	static void do_complete(void *owner, win_iocp_operation *base,
							async_error_code error, size_t bytes);

	HANDLE handle_;
	win_iocp_operation *target_;
	cancellation_slot slot_;
	volatile LONG *cancel_requested_;
	volatile LONG refs_;
};

#endif /* _ASYNC_OP_IOCP_OP_CANCELLATION_H_ */
