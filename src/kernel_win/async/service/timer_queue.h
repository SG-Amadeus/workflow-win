/*
  AsyncCore: non-template timer_queue.

  Port of asio::detail::timer_queue with templates removed.
  wait_ops are stored in an intrusive op_queue of win_iocp_operation.
*/

#ifndef _ASYNC_TIMER_QUEUE_H_
#define _ASYNC_TIMER_QUEUE_H_

#include "../op/timer_op.h"
#include "../op/op_queue.h"
#include "../error.h"

#include <chrono>
#include <stddef.h>

#include "../../list.h"

class timer_per_timer_data
{
public:
	struct list_head list;
	op_queue wait_ops;
	size_t heap_index;
};

class timer_heap_entry
{
public:
	std::chrono::steady_clock::time_point time;
	timer_per_timer_data *timer;
};

class timer_queue
{
public:
	struct list_head timers;
	struct list_head set_list;
	timer_heap_entry *heap;
	size_t heap_size;
	size_t heap_capacity;
};

void timer_queue_init(timer_queue *q);
void timer_queue_destroy(timer_queue *q);
void timer_per_timer_data_init(timer_per_timer_data *timer);

bool timer_queue_empty(const timer_queue *q);

int timer_queue_enqueue(timer_queue *q,
						const std::chrono::steady_clock::time_point &time,
						timer_per_timer_data *timer, wait_op *op);

long timer_queue_wait_duration_msec(const timer_queue *q, long max_duration);
long timer_queue_wait_duration_usec(const timer_queue *q, long max_duration);

/* Move all expired wait_ops to ops (an op_queue). */
void timer_queue_get_ready_timers(timer_queue *q, op_queue &ops);

/* Move all pending wait_ops to ops for shutdown. */
void timer_queue_get_all_timers(timer_queue *q, op_queue &ops);

/* Cancel up to max_cancelled wait_ops on one timer.  Cancelled ops have their
 * error set to system_category::operation_aborted and are moved to ops.
 * Returns the count. */
size_t timer_queue_cancel_timer(timer_queue *q, timer_per_timer_data *timer,
								op_queue &ops,
								size_t max_cancelled = (size_t)-1);

/* Cancel only the wait op whose cancellation_key matches. */
void timer_queue_cancel_timer_by_key(timer_queue *q,
									 timer_per_timer_data *timer,
									 op_queue &ops, void *key);

void timer_queue_move_timer(timer_queue *q, timer_per_timer_data *target,
							timer_per_timer_data *source);

#endif /* _ASYNC_TIMER_QUEUE_H_ */

