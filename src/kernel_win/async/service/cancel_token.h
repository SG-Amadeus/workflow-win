/*
  AsyncCore: shared socket/handle cancellation state.

  This is the non-template equivalent of ASIO's
  socket_ops::shared_cancel_token_type.  The resource implementation owns one
  reference and every pending operation owns another.  This keeps cancellation
  state alive after the public handle has been destroyed.
*/

#ifndef _ASYNC_SERVICE_CANCEL_TOKEN_H_
#define _ASYNC_SERVICE_CANCEL_TOKEN_H_

#include <Windows.h>
#include <stdlib.h>
#include <new>

class cancel_token
{
public:
	cancel_token() : refs_(1), closed_(0) {}

	static cancel_token *create()
	{
		void *mem = malloc(sizeof(cancel_token));
		return mem ? new (mem) cancel_token() : nullptr;
	}

	void acquire()
	{
		::InterlockedIncrement(&refs_);
	}

	void release()
	{
		if (::InterlockedDecrement(&refs_) == 0)
		{
			this->~cancel_token();
			free(this);
		}
	}

	void close()
	{
		::InterlockedExchange(&closed_, 1);
	}

	void reset()
	{
		::InterlockedExchange(&closed_, 0);
	}

	bool is_closed() const
	{
		return ::InterlockedCompareExchange(
			const_cast<volatile LONG *>(&closed_), 0, 0) != 0;
	}

private:
	volatile LONG refs_;
	volatile LONG closed_;
};

#endif /* _ASYNC_SERVICE_CANCEL_TOKEN_H_ */

