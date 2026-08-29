#include "timer_queue_set.h"

#include "../../list.h"

void timer_queue_set_init(timer_queue_set *set)
{
	INIT_LIST_HEAD(&set->queues);
}

void timer_queue_set_insert(timer_queue_set *set, timer_queue *queue)
{
	list_add_tail(&queue->set_list, &set->queues);
}

void timer_queue_set_erase(timer_queue_set *set, timer_queue *queue)
{
	(void)set;
	if (!list_empty(&queue->set_list))
	{
		list_del(&queue->set_list);
		INIT_LIST_HEAD(&queue->set_list);
	}
}

bool timer_queue_set_empty(const timer_queue_set *set)
{
	return list_empty(&set->queues);
}

long timer_queue_set_wait_duration_msec(const timer_queue_set *set,
										long max_duration)
{
	long duration = max_duration;
	struct list_head *pos;

	list_for_each(pos, const_cast<struct list_head *>(&set->queues))
	{
		timer_queue *queue = list_entry(pos, timer_queue, set_list);
		duration = timer_queue_wait_duration_msec(queue, duration);
	}
	return duration;
}

long timer_queue_set_wait_duration_usec(const timer_queue_set *set,
										long max_duration)
	{
	long duration = max_duration;
	struct list_head *pos;

	list_for_each(pos, const_cast<struct list_head *>(&set->queues))
	{
		timer_queue *queue = list_entry(pos, timer_queue, set_list);
		duration = timer_queue_wait_duration_usec(queue, duration);
	}
	return duration;
}

void timer_queue_set_get_ready_timers(timer_queue_set *set, op_queue &ops)
{
	struct list_head *pos;

	list_for_each(pos, &set->queues)
	{
		timer_queue *queue = list_entry(pos, timer_queue, set_list);
		timer_queue_get_ready_timers(queue, ops);
	}
}

void timer_queue_set_get_all_timers(timer_queue_set *set, op_queue &ops)
{
	struct list_head *pos;

	list_for_each(pos, &set->queues)
	{
		timer_queue *queue = list_entry(pos, timer_queue, set_list);
		timer_queue_get_all_timers(queue, ops);
	}
}
