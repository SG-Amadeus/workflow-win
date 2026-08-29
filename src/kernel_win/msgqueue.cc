/*
  Copyright (c) 2020 Sogou, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
*/

#include <errno.h>
#include <stdlib.h>
#include <WinSock2.h>
#include <Windows.h>
#include "msgqueue.h"

struct __msgqueue
{
	size_t msg_max;
	size_t msg_cnt;
	int linkoff;
	int nonblock;
	PVOID volatile head1;
	PVOID volatile head2;
	PVOID volatile *get_head;
	PVOID volatile *put_head;
	PVOID volatile *put_tail;
	SRWLOCK get_lock;
	SRWLOCK put_lock;
	CONDITION_VARIABLE get_cond;
	CONDITION_VARIABLE put_cond;
};

static void *msgqueue_load_head(PVOID const volatile *head)
{
	return ReadPointerNoFence(head);
}

static void msgqueue_store_head(PVOID volatile *head, void *value)
{
	WritePointerNoFence(head, value);
}

void msgqueue_set_nonblock(msgqueue_t *queue)
{
	AcquireSRWLockExclusive(&queue->put_lock);
	queue->nonblock = 1;
	WakeAllConditionVariable(&queue->get_cond);
	WakeAllConditionVariable(&queue->put_cond);
	ReleaseSRWLockExclusive(&queue->put_lock);
}

void msgqueue_set_block(msgqueue_t *queue)
{
	AcquireSRWLockExclusive(&queue->put_lock);
	queue->nonblock = 0;
	ReleaseSRWLockExclusive(&queue->put_lock);
}

void msgqueue_put(void *msg, msgqueue_t *queue)
{
	void **link = (void **)((char *)msg + queue->linkoff);

	*link = NULL;
	AcquireSRWLockExclusive(&queue->put_lock);
	while (queue->msg_max != 0 && queue->msg_cnt >= queue->msg_max &&
		   !queue->nonblock)
	{
		SleepConditionVariableSRW(&queue->put_cond, &queue->put_lock,
								  INFINITE, 0);
	}

	msgqueue_store_head(queue->put_tail, link);
	queue->put_tail = (PVOID volatile *)link;
	queue->msg_cnt++;
	WakeConditionVariable(&queue->get_cond);
	ReleaseSRWLockExclusive(&queue->put_lock);
}

void msgqueue_put_head(void *msg, msgqueue_t *queue)
{
	void **link = (void **)((char *)msg + queue->linkoff);
	void *head;

	/*
	 * Linux-compatible priority insertion.  While a consumer batch exists
	 * (get_head non-empty), keep trying to splice the message in front of
	 * it: put_lock stabilizes get_head, and a consumer holding get_lock
	 * with a non-empty batch is only unlinking one element (microseconds)
	 * -- it can never be waiting on put_lock, because the swap that takes
	 * put_lock runs only when get_head is empty.  The loop therefore ends
	 * on the first successful trylock, or as soon as the consumer batch
	 * drains (unlink by unlink; it cannot be refilled while put_lock is
	 * held) and the message falls back to the producer batch (blocking on
	 * a full bounded queue, like Linux).  No hard bound is promised,
	 * though: the trylock is not a fair acquisition, so under sustained
	 * adversarial contention a producer could in theory spin -- the same
	 * theoretical property as Linux's pthread_mutex_trylock, accepted
	 * here to keep the original priority-insertion semantics (relied on
	 * by thrdpool_decrease()).
	 */
	AcquireSRWLockExclusive(&queue->put_lock);
	head = msgqueue_load_head(queue->get_head);
	while (head)
	{
		if (TryAcquireSRWLockExclusive(&queue->get_lock))
		{
			/*
			 * Trylock succeeded, but re-read anyway: a consumer may have
			 * advanced the batch between the loop's load above and the
			 * trylock, in which case head names an element already handed
			 * out, and splicing it back would double-free it.  The re-read
			 * is authoritative (no consumer can unlink while we hold
			 * get_lock, and the swap needs put_lock which we hold).
			 */
			head = msgqueue_load_head(queue->get_head);
			if (head)
			{
				*link = head;
				msgqueue_store_head(queue->get_head, link);
				ReleaseSRWLockExclusive(&queue->get_lock);
				ReleaseSRWLockExclusive(&queue->put_lock);
				return;
			}

			ReleaseSRWLockExclusive(&queue->get_lock);
		}

		head = msgqueue_load_head(queue->get_head);
	}

	while (queue->msg_max != 0 && queue->msg_cnt >= queue->msg_max &&
		   !queue->nonblock)
	{
		SleepConditionVariableSRW(&queue->put_cond, &queue->put_lock,
								  INFINITE, 0);
	}

	*link = msgqueue_load_head(queue->put_head);
	if (!*link)
		queue->put_tail = (PVOID volatile *)link;

	msgqueue_store_head(queue->put_head, link);
	queue->msg_cnt++;
	WakeConditionVariable(&queue->get_cond);
	ReleaseSRWLockExclusive(&queue->put_lock);
}

static size_t msgqueue_swap(msgqueue_t *queue)
{
	PVOID volatile *get_head = queue->get_head;
	size_t count;

	AcquireSRWLockExclusive(&queue->put_lock);
	while (queue->msg_cnt == 0 && !queue->nonblock)
	{
		SleepConditionVariableSRW(&queue->get_cond, &queue->put_lock,
								  INFINITE, 0);
	}

	count = queue->msg_cnt;
	if (queue->msg_max != 0 && count >= queue->msg_max)
		WakeAllConditionVariable(&queue->put_cond);

	queue->get_head = queue->put_head;
	queue->put_head = get_head;
	queue->put_tail = get_head;
	queue->msg_cnt = 0;
	ReleaseSRWLockExclusive(&queue->put_lock);
	return count;
}

void *msgqueue_get(msgqueue_t *queue)
{
	void *msg;
	void *head;

	AcquireSRWLockExclusive(&queue->get_lock);
	head = msgqueue_load_head(queue->get_head);
	if (!head && msgqueue_swap(queue) > 0)
		head = msgqueue_load_head(queue->get_head);

	if (head)
	{
		msg = (char *)head - queue->linkoff;
		msgqueue_store_head(queue->get_head, *(void **)head);
	}
	else
		msg = NULL;

	ReleaseSRWLockExclusive(&queue->get_lock);
	return msg;
}

msgqueue_t *msgqueue_create(size_t maxlen, int linkoff)
{
	msgqueue_t *queue;

	/*
	 * linkoff is the intrusive-link offset; like Linux, any value the
	 * caller guarantees valid for its message type is accepted (the
	 * pointer arithmetic happens at put/get time, not here).
	 */
	queue = (msgqueue_t *)malloc(sizeof (msgqueue_t));
	if (!queue)
		return NULL;

	queue->msg_max = maxlen;
	queue->msg_cnt = 0;
	queue->linkoff = linkoff;
	queue->nonblock = 0;
	queue->head1 = NULL;
	queue->head2 = NULL;
	queue->get_head = &queue->head1;
	queue->put_head = &queue->head2;
	queue->put_tail = &queue->head2;
	InitializeSRWLock(&queue->get_lock);
	InitializeSRWLock(&queue->put_lock);
	InitializeConditionVariable(&queue->get_cond);
	InitializeConditionVariable(&queue->put_cond);
	return queue;
}

void msgqueue_destroy(msgqueue_t *queue)
{
	free(queue);
}

