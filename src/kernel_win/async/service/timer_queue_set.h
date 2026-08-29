/*
 * AsyncCore: timer queue set used by the IOCP scheduler.
 *
 * Non-template equivalent of asio::detail::timer_queue_set.
 */

#ifndef _ASYNC_TIMER_QUEUE_SET_H_
#define _ASYNC_TIMER_QUEUE_SET_H_

#include "timer_queue.h"

class timer_queue_set
{
public:
	struct list_head queues;
};

void timer_queue_set_init(timer_queue_set *set);
void timer_queue_set_insert(timer_queue_set *set, timer_queue *queue);
void timer_queue_set_erase(timer_queue_set *set, timer_queue *queue);
bool timer_queue_set_empty(const timer_queue_set *set);
long timer_queue_set_wait_duration_msec(const timer_queue_set *set,
										long max_duration);
long timer_queue_set_wait_duration_usec(const timer_queue_set *set,
										long max_duration);
void timer_queue_set_get_ready_timers(timer_queue_set *set, op_queue &ops);
void timer_queue_set_get_all_timers(timer_queue_set *set, op_queue &ops);

#endif /* _ASYNC_TIMER_QUEUE_SET_H_ */
