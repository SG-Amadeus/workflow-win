/*
 * AsyncCore: ASIO-style composed operation for Communicator sleep.
 *
 * This is the timer equivalent of asio::steady_timer::async_wait wrapped as a
 * business completion handler.  It owns the ASIO timer and delivers the
 * final SleepSession::handle() callback.
 */

#ifndef _ASYNC_OP_COMM_SLEEP_OP_H_
#define _ASYNC_OP_COMM_SLEEP_OP_H_

#include "../async/op/composed_op.h"
#include "comm_conn.h"

#include "../thrdpool.h"

class comm_sleep_op : public composed_op
{
	public:
	steady_timer timer_;
	CommunicatorImpl *impl_;
	SleepSession *session_;
	struct list_head live_list_;
	int result_state_;
	int result_error_;
	volatile LONG handler_completed_;
	struct thrdpool_task_entry handler_task_;

	comm_sleep_op();

	static void destroy(composed_op *base);
	static void complete(composed_op *base);
	static void retire_io(comm_sleep_op *self, async_error_code error,
						  bool destroyed);
	static void timer_cb(void *ctx, async_error_code error);
	static void handle_complete(void *ctx);
	static void post_completion(comm_sleep_op *self, async_error_code error,
								bool destroyed);
	static int cancel(SleepSession *session);
	static void cancel_all(CommunicatorImpl *impl);
	static int start(CommunicatorImpl *impl, SleepSession *session,
					 long long ms);
};

#endif /* _ASYNC_OP_COMM_SLEEP_OP_H_ */


