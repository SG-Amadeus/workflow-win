#include "timer_queue.h"

#include <errno.h>
#include <stdlib.h>

namespace
{

void swap_heap(timer_queue *q, size_t i1, size_t i2)
{
	timer_heap_entry tmp = q->heap[i1];
	q->heap[i1] = q->heap[i2];
	q->heap[i2] = tmp;
	q->heap[i1].timer->heap_index = i1;
	q->heap[i2].timer->heap_index = i2;
}

void up_heap(timer_queue *q, size_t index)
{
	while (index > 0)
	{
		size_t parent = (index - 1) / 2;
		if (!(q->heap[index].time < q->heap[parent].time))
			break;
		swap_heap(q, index, parent);
		index = parent;
	}
}

void down_heap(timer_queue *q, size_t index)
{
	size_t child = index * 2 + 1;
	while (child < q->heap_size)
	{
		size_t min_child = child;
		if (child + 1 < q->heap_size &&
			q->heap[child + 1].time < q->heap[child].time)
			min_child = child + 1;
		if (q->heap[index].time < q->heap[min_child].time)
			break;
		swap_heap(q, index, min_child);
		index = min_child;
		child = index * 2 + 1;
	}
}

void remove_timer(timer_queue *q, timer_per_timer_data *timer)
{
	size_t index = timer->heap_index;
	if (q->heap_size > 0 && index < q->heap_size)
	{
		if (index == q->heap_size - 1)
		{
			timer->heap_index = (size_t)-1;
			--q->heap_size;
		}
		else
		{
			swap_heap(q, index, q->heap_size - 1);
			timer->heap_index = (size_t)-1;
			--q->heap_size;
			if (index > 0 &&
				q->heap[index].time < q->heap[(index - 1) / 2].time)
				up_heap(q, index);
			else
				down_heap(q, index);
		}
	}

	list_del(&timer->list);
	INIT_LIST_HEAD(&timer->list);
}

} /* namespace */

void timer_queue_init(timer_queue *q)
{
	INIT_LIST_HEAD(&q->timers);
	INIT_LIST_HEAD(&q->set_list);
	q->heap = nullptr;
	q->heap_size = 0;
	q->heap_capacity = 0;
}

void timer_per_timer_data_init(timer_per_timer_data *timer)
{
	INIT_LIST_HEAD(&timer->list);
	timer->heap_index = (size_t)-1;
}

void timer_queue_destroy(timer_queue *q)
{
	free(q->heap);
	q->heap = nullptr;
	q->heap_size = 0;
	q->heap_capacity = 0;
	INIT_LIST_HEAD(&q->timers);
	INIT_LIST_HEAD(&q->set_list);
}

bool timer_queue_empty(const timer_queue *q)
{
	return list_empty(&q->timers);
}

int timer_queue_enqueue(timer_queue *q,
						const std::chrono::steady_clock::time_point &time,
						timer_per_timer_data *timer, wait_op *op)
{
	bool is_new_timer = list_empty(&timer->list);
	if (is_new_timer)
	{
		if (q->heap_size == q->heap_capacity)
		{
			if (q->heap_capacity > (size_t)-1 / 2)
			{
				errno = ENOMEM;
				return -1;
			}
			size_t new_capacity = q->heap_capacity ? q->heap_capacity * 2 : 8;
			if (new_capacity > (size_t)-1 / sizeof(timer_heap_entry))
			{
				errno = ENOMEM;
				return -1;
			}
			timer_heap_entry *new_heap = static_cast<timer_heap_entry *>(
				realloc(q->heap, new_capacity * sizeof(timer_heap_entry)));
			if (!new_heap)
			{
				errno = ENOMEM;
				return -1;
			}
			q->heap = new_heap;
			q->heap_capacity = new_capacity;
		}

		timer->heap_index = q->heap_size;
		q->heap[q->heap_size].time = time;
		q->heap[q->heap_size].timer = timer;
		++q->heap_size;
		up_heap(q, q->heap_size - 1);

		list_add_tail(&timer->list, &q->timers);
	}

	timer->wait_ops.push(op);
	return (timer->heap_index == 0 &&
			timer->wait_ops.front() == op) ? 1 : 0;
}

long timer_queue_wait_duration_msec(const timer_queue *q, long max_duration)
{
	if (q->heap_size == 0)
		return max_duration;

	std::chrono::steady_clock::time_point now =
		std::chrono::steady_clock::now();
	if (q->heap[0].time <= now)
		return 0;

	long long ms = std::chrono::duration_cast<std::chrono::milliseconds>(
		q->heap[0].time - now).count();
	if (ms == 0)
		return 1;
	if (ms > max_duration)
		return max_duration;
	return static_cast<long>(ms);
}

long timer_queue_wait_duration_usec(const timer_queue *q, long max_duration)
{
	if (q->heap_size == 0)
		return max_duration;

	std::chrono::steady_clock::time_point now =
		std::chrono::steady_clock::now();
	if (q->heap[0].time <= now)
		return 0;

	long long usec = std::chrono::duration_cast<std::chrono::microseconds>(
		q->heap[0].time - now).count();
	if (usec == 0)
		return 1;
	if (usec > max_duration)
		return max_duration;
	return static_cast<long>(usec);
}

void timer_queue_get_ready_timers(timer_queue *q, op_queue &ops)
{
	std::chrono::steady_clock::time_point now =
		std::chrono::steady_clock::now();

	while (q->heap_size > 0 && !(now < q->heap[0].time))
	{
		timer_per_timer_data *timer = q->heap[0].timer;
		while (!timer->wait_ops.empty())
		{
			win_iocp_operation *base = timer->wait_ops.front();
			timer->wait_ops.pop();
			wait_op *op = (wait_op *)(void *)base;
			op->error.clear();
			ops.push(base);
		}
		remove_timer(q, timer);
	}
}

void timer_queue_get_all_timers(timer_queue *q, op_queue &ops)
{
	while (!list_empty(&q->timers))
	{
		timer_per_timer_data *timer =
			list_entry(q->timers.next, timer_per_timer_data, list);
		list_del(&timer->list);
		INIT_LIST_HEAD(&timer->list);
		ops.push(timer->wait_ops);
		timer->heap_index = (size_t)-1;
	}
	q->heap_size = 0;
}

size_t timer_queue_cancel_timer(timer_queue *q, timer_per_timer_data *timer,
								op_queue &ops, size_t max_cancelled)
{
	bool active = !list_empty(&timer->list);
	if (!active)
		return 0;

	size_t count = 0;
	while (count < max_cancelled && !timer->wait_ops.empty())
	{
		win_iocp_operation *base = timer->wait_ops.front();
		timer->wait_ops.pop();
		wait_op *op = (wait_op *)(void *)base;
		op->error = async_system_error(ERROR_OPERATION_ABORTED);
		ops.push(base);
		++count;
	}

	if (timer->wait_ops.empty())
		remove_timer(q, timer);

	return count;
}

void timer_queue_cancel_timer_by_key(timer_queue *q,
									 timer_per_timer_data *timer,
									 op_queue &ops, void *key)
{
	bool active = !list_empty(&timer->list);
	if (!active)
		return;

	op_queue other;

	while (!timer->wait_ops.empty())
	{
		win_iocp_operation *base = timer->wait_ops.front();
		timer->wait_ops.pop();
		wait_op *op = (wait_op *)(void *)base;
		if (op->cancellation_key == key)
		{
			op->error = async_system_error(ERROR_OPERATION_ABORTED);
			ops.push(base);
		}
		else
		{
			other.push(base);
		}
	}

	timer->wait_ops.push(other);
	if (timer->wait_ops.empty())
		remove_timer(q, timer);
}

void timer_queue_move_timer(timer_queue *q, timer_per_timer_data *target,
							timer_per_timer_data *source)
{
	target->wait_ops.push(source->wait_ops);

	target->heap_index = source->heap_index;
	source->heap_index = (size_t)-1;

	if (target->heap_index < q->heap_size)
		q->heap[target->heap_index].timer = target;

	if (!list_empty(&source->list))
	{
		struct list_head *prev = source->list.prev;
		struct list_head *next = source->list.next;
		prev->next = &target->list;
		next->prev = &target->list;
		target->list.prev = prev;
		target->list.next = next;
		INIT_LIST_HEAD(&source->list);
	}
}

