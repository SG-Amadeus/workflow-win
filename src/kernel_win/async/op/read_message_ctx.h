/*
  AsyncCore: read-message composed state, non-template.

  This is the explicit state machine behind async_read_message_ex /
  async_read_message_ssl_ex.  It lives in the op layer so Communicator does
  not need to know the timer/read arbitration details.
*/

#ifndef _ASYNC_OP_READ_MESSAGE_CTX_H_
#define _ASYNC_OP_READ_MESSAGE_CTX_H_

#include "../executor.h"
#include "../steady_timer.h"
#include "message.h"

#include <Windows.h>

#include <stddef.h>

class read_message_ctx;

class read_timer_wait
{
public:
	read_message_ctx *ctx;
	LONG generation;
};

typedef int (*read_submit_t)(void *, void *, size_t,
	void (*)(void *, async_error_code, size_t), void *, const cancellation_slot &,
	void (*)(void *));
typedef executor (*read_executor_t)(void *);

int async_start_read_message(void *socket, read_submit_t submit,
							 read_executor_t get_executor,
							 void *buffer, size_t size,
							 read_message_filter filter,
							 void *filter_userdata,
							 void (*callback)(void *, async_error_code, size_t),
							 void *context, int first_timeout,
							 int next_timeout, int total_timeout,
							 const cancellation_slot &slot,
							 void (*destroy)(void *),
							 volatile LONG *renew_flag);

class read_message_ctx
{
public:
	void *socket;
	read_submit_t submit;
	cancellation_state cancel_state;
	cancellation_signal read_cancel;
	void *buffer;
	void *owned_buffer;
	size_t size;
	size_t pending;
	read_message_filter filter;
	void *filter_userdata;
	void (*callback)(void *, async_error_code, size_t);
	void (*destroy)(void *);
	void *context;
	steady_timer *timer;
	int first_timeout;
	int next_timeout;
	int total_timeout;
	ULONGLONG total_deadline;
	bool first_phase;
	volatile LONG refs;
	volatile LONG done;
	volatile LONG destroy_called;
	LONG timer_generation;
	volatile LONG *renew_flag;
	executor executor_;
	async_error_code result_error;
	size_t result_bytes;
	CRITICAL_SECTION lock;
};

#endif /* _ASYNC_OP_READ_MESSAGE_CTX_H_ */

