/*
  AsyncCore: ASIO win_iocp_operation, de-templated.

  The concrete operation is named after ASIO.  This is the only IOCP
  operation base used by AsyncCore.
*/

#ifndef _ASYNC_WIN_IOCP_OPERATION_H_
#define _ASYNC_WIN_IOCP_OPERATION_H_

#include <WinSock2.h>
#include <Windows.h>
#include <stddef.h>

#include "../error.h"

class win_iocp_operation : public OVERLAPPED
{
	public:
	typedef win_iocp_operation operation_type;

	void complete(void *owner, async_error_code error, size_t bytes)
	{
		func_(owner, this, error, bytes);
	}

	void destroy()
	{
		func_(nullptr, this, async_error_code(), 0);
	}

	void reset()
	{
		Internal = 0;
		InternalHigh = 0;
		Offset = 0;
		OffsetHigh = 0;
		hEvent = nullptr;
		ready_ = 0;
	}

	protected:
	typedef void (*func_type)(void *owner, win_iocp_operation *op,
								 async_error_code error, size_t bytes);

	win_iocp_operation(func_type func)
		: next_(nullptr), func_(func), ready_(0)
	{
		reset();
	}

	~win_iocp_operation() {}

	private:
	friend class op_queue_access;
	friend class io_context;
	win_iocp_operation *next_;
	func_type func_;
	LONG ready_;

};

static_assert(offsetof(win_iocp_operation, Internal) == 0,
			  "win_iocp_operation must inherit OVERLAPPED at offset 0");

static inline win_iocp_operation *win_iocp_operation_from_overlapped(
	OVERLAPPED *overlapped)
{
	return static_cast<win_iocp_operation *>(overlapped);
}

#endif /* _ASYNC_WIN_IOCP_OPERATION_H_ */
