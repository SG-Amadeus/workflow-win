/*
  AsyncCore: non-template steady timer, ported from ASIO timer_queue.

  steady_timer is the equivalent of asio::steady_timer.
  It supports multiple pending async_wait operations, expiry changes that
  cancel pending waits, and cancel() returning the number cancelled.
*/

#ifndef _ASYNC_STEADY_TIMER_H_
#define _ASYNC_STEADY_TIMER_H_

#include "executor.h"

#include <chrono>
#include <cstddef>

class steady_timer
{
public:
	steady_timer();
	~steady_timer();
	static steady_timer *create(executor ex);
	static void destroy(steady_timer *timer);
	int init(executor ex);

	steady_timer(const steady_timer &) = delete;
	steady_timer &operator=(const steady_timer &) = delete;
	/* Returns the number of pending waits cancelled by the change. */
	size_t expires_after(std::chrono::milliseconds ms);
	size_t expires_at(std::chrono::steady_clock::time_point tp);

	/* Returns the number of pending waits cancelled. */
	size_t cancel();
	executor get_executor() const;

	/* Callback receives success or system_category::operation_aborted when
	 * cancel() wins, matching asio::steady_timer::async_wait. */
	int async_wait(void (*callback)(void *, async_error_code error), void *context);
	int async_wait(void (*callback)(void *, async_error_code error), void *context,
				   void (*destroy)(void *));

	class impl;
	impl *impl_;

private:
};

/* The timer operation releases its temporary impl reference after the
 * completion handler has been detached from the operation. */
void steady_timer_impl_release(steady_timer::impl *impl);

#endif /* _ASYNC_STEADY_TIMER_H_ */

