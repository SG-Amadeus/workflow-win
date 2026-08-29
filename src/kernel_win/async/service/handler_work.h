/*
  AsyncCore: non-template handler_work.

  This is the simplified non-template equivalent of
  asio::detail::handler_work<Handler, IoExecutor>.

  The native io_context completion already owns the operation's work.  A
  handler bound to a strand needs an additional tracked reference while the
  completion is waiting to enter that strand.  This type carries that rule
  without exposing ASIO's handler templates.
*/

#ifndef _ASYNC_HANDLER_WORK_H_
#define _ASYNC_HANDLER_WORK_H_

#include "../executor.h"

class handler_work
{
public:
	handler_work();
	explicit handler_work(executor ex);
	handler_work(const handler_work &other);
	handler_work(handler_work &&other);
	handler_work &operator=(const handler_work &other);
	handler_work &operator=(handler_work &&other);
	~handler_work();

	/* Complete the operation through its associated executor.  destroy is
	 * invoked if the handler is abandoned by io_context shutdown. */
	int complete(void (*routine)(void *), void *context,
				 void (*destroy)(void *) = nullptr);
	int post(void (*routine)(void *), void *context,
			 void (*destroy)(void *) = nullptr);
	int defer(void (*routine)(void *), void *context,
			  void (*destroy)(void *) = nullptr);

	executor get_executor() const;
	bool owns_work() const;
	void reset();

private:
	void acquire_work();
	void release_work();

	executor executor_;
	bool owns_work_;
};

#endif /* _ASYNC_HANDLER_WORK_H_ */

