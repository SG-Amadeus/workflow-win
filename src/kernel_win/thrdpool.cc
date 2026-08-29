/*
  Copyright (c) 2019 Sogou, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.

  Windows port of src/kernel/thrdpool.c (Sogou Workflow).  The queue,
  exit-task, join-chain and destroy-from-worker algorithms are kept; pthread
  mutex/condition/TLS/thread calls are replaced.  Deliberate differences
  from the Linux version:

   - Termination state is pool-owned: a permanent `terminating` flag plus a
     CONDITION_VARIABLE in the pool, instead of Linux's pointer to a
     terminator's stack CV.  No termination wait can reference a stack
     object, and the flag is never reset (a terminated pool can never
     restart, matching Linux).

   - Thread creation is reserve-before-create: nthreads is incremented
     before _beginthreadex and rolled back on failure.  _beginthreadex may
     run the worker before returning, and a worker that observes
     nthreads == 0 treats itself as the destroy-from-worker survivor and
     frees the pool; counting first closes that window.  The rollback and
     the worker's own exit decrement both hold pool->mutex, so the counter
     cannot transiently read 0 for a live worker.

   - thrdpool_create(0, ...) is accepted (Linux-compatible empty pool that
     increase() fills later); stacksize > UINT_MAX is rejected up front
     because _beginthreadex takes the stack size as unsigned.

   - Unrecoverable kernel failures (DuplicateHandle, join wait, TLS setup)
     raise RaiseFailFastException.  There is no safe fallback: an exit
     thread without a join handle cannot keep the join-chain invariant, so
     silently exiting would leak the join wait or free the pool under a
     running thread.  FailFast terminates the process rather than corrupt
     concurrency state; it is an internal-invariant policy, distinct from
     the errno-returning public API.

   - increase() maps a missing or stale errno from a failed _beginthreadex
     to EAGAIN, so callers always observe a defined error.

   - Workers are created with _beginthreadex/_endthreadex; the routine must
     return unsigned (int), not DWORD, to match _beginthreadex_proc_type.

   - Worker exit costs more than pthread_self(): each exiting worker
     DuplicateHandles its own thread, updates pool->tid under the lock,
     joins the previous worker and closes its handle.  This affects only
     resize and destroy, never the task hot path.

   - A task that terminates its thread without returning (exit(),
     TerminateThread) leaves nthreads unbalanced and destroy() may wait
     forever.  This matches Linux; the task contract is unchanged.
*/

#include <errno.h>
#include <limits.h>
#include <process.h>
#include <stdint.h>
#include <stdlib.h>
#include <WinSock2.h>
#include <Windows.h>
#include "msgqueue.h"
#include "thrdpool.h"

struct __thrdpool
{
	msgqueue_t *msgqueue;
	volatile LONG nthreads;
	size_t stacksize;
	HANDLE tid;
	SRWLOCK mutex;
	DWORD key;
	/*
	 * Termination is a permanent lifecycle state, not a pointer to a
	 * caller-owned object: once the pool is terminated it can never be
	 * restarted, so the flag is never reset.  term_cond is owned by the
	 * pool itself, so no termination wait can reference a stack object.
	 */
	volatile LONG terminating;
	CONDITION_VARIABLE term_cond;
};

static LONG __thrdpool_thread_count(thrdpool_t *pool)
{
	return InterlockedCompareExchange(&pool->nthreads, 0, 0);
}

static int __thrdpool_termination(thrdpool_t *pool)
{
	return (int)InterlockedCompareExchange(&pool->terminating, 0, 0);
}

static __declspec(noreturn) void __thrdpool_exit_routine(void *context)
{
	thrdpool_t *pool = (thrdpool_t *)context;
	HANDLE previous;
	HANDLE current = NULL;

	if (!DuplicateHandle(GetCurrentProcess(), GetCurrentThread(),
						 GetCurrentProcess(), &current, SYNCHRONIZE,
						 FALSE, 0))
	{
		RaiseFailFastException(NULL, NULL, 0);
	}

	/* One thread joins another.  No worker registry is needed. */
	AcquireSRWLockExclusive(&pool->mutex);
	previous = pool->tid;
	pool->tid = current;
	if (InterlockedDecrement(&pool->nthreads) == 0 &&
		__thrdpool_termination(pool))
	{
		WakeConditionVariable(&pool->term_cond);
	}

	ReleaseSRWLockExclusive(&pool->mutex);
	if (previous)
	{
		if (WaitForSingleObject(previous, INFINITE) != WAIT_OBJECT_0)
			RaiseFailFastException(NULL, NULL, 0);

		CloseHandle(previous);
	}

	TlsSetValue(pool->key, NULL);
	_endthreadex(0);
}

/*
 * Return type must be unsigned (int), not DWORD: _beginthreadex expects
 * _beginthreadex_proc_type which returns unsigned int, and MSVC treats
 * DWORD (unsigned long) as a distinct type for function pointers.
 */
static unsigned __stdcall __thrdpool_routine(void *arg)
{
	thrdpool_t *pool = (thrdpool_t *)arg;
	struct thrdpool_task_entry *entry;
	void (*task_routine)(void *);
	void *task_context;
	int allocated;

	if (!TlsSetValue(pool->key, pool))
		RaiseFailFastException(NULL, NULL, 0);

	while (!__thrdpool_termination(pool))
	{
		entry = (struct thrdpool_task_entry *)msgqueue_get(pool->msgqueue);
		if (!entry)
			break;

		task_routine = entry->task.routine;
		task_context = entry->task.context;
		allocated = entry->allocated;
		if (allocated)
			free(entry);
		task_routine(task_context);

		if (__thrdpool_thread_count(pool) == 0)
		{
			/* The task destroyed the pool from this worker. */
			free(pool);
			return 0;
		}
	}

	__thrdpool_exit_routine(pool);
}

static void __thrdpool_terminate(int in_pool, thrdpool_t *pool)
{
	AcquireSRWLockExclusive(&pool->mutex);
	msgqueue_set_nonblock(pool->msgqueue);
	InterlockedExchange(&pool->terminating, 1);

	if (in_pool)
		InterlockedDecrement(&pool->nthreads);

	while (__thrdpool_thread_count(pool) > 0)
	{
		SleepConditionVariableSRW(&pool->term_cond, &pool->mutex,
								  INFINITE, 0);
	}

	ReleaseSRWLockExclusive(&pool->mutex);
	if (pool->tid)
	{
		if (WaitForSingleObject(pool->tid, INFINITE) != WAIT_OBJECT_0)
			RaiseFailFastException(NULL, NULL, 0);

		CloseHandle(pool->tid);
		pool->tid = NULL;
	}
}

static int __thrdpool_create_threads(size_t nthreads, thrdpool_t *pool)
{
	DWORD flags = pool->stacksize ? STACK_SIZE_PARAM_IS_A_RESERVATION : 0;
	int error = 0;

	if (nthreads > LONG_MAX)
	{
		errno = EINVAL;
		return -1;
	}

	AcquireSRWLockExclusive(&pool->mutex);
	while ((size_t)__thrdpool_thread_count(pool) < nthreads)
	{
		/*
		 * Register the thread before creating it: _beginthreadex without
		 * CREATE_SUSPENDED may run the worker immediately, and a worker
		 * that sees nthreads == 0 treats itself as the last destroy-
		 * from-worker survivor and frees the pool.  Counting first closes
		 * that window; a failed create rolls the count back.  Both the
		 * rollback and the worker's own exit decrement hold pool->mutex,
		 * so the counter cannot transiently read 0 for a live worker.
		 */
		InterlockedIncrement(&pool->nthreads);
		uintptr_t thread = _beginthreadex(NULL, (unsigned)pool->stacksize,
										  __thrdpool_routine, pool, flags,
										  NULL);

		if (!thread)
		{
			/* errno is already set by the CRT. */
			error = errno;
			InterlockedDecrement(&pool->nthreads);
			break;
		}

		CloseHandle((HANDLE)thread);
	}

	ReleaseSRWLockExclusive(&pool->mutex);
	if ((size_t)__thrdpool_thread_count(pool) == nthreads)
		return 0;

	__thrdpool_terminate(0, pool);
	errno = error ? error : EAGAIN;
	return -1;
}

thrdpool_t *thrdpool_create(size_t nthreads, size_t stacksize)
{
	thrdpool_t *pool;

	if (stacksize > UINT_MAX)
	{
		/*
		 * stacksize is passed to _beginthreadex as unsigned; reject values
		 * that would silently truncate instead of creating a wrong pool.
		 */
		errno = EINVAL;
		return NULL;
	}

	pool = (thrdpool_t *)malloc(sizeof (thrdpool_t));
	if (!pool)
		return NULL;

	pool->msgqueue = msgqueue_create(0, 0);
	if (pool->msgqueue)
	{
		InitializeSRWLock(&pool->mutex);
		pool->key = TlsAlloc();
		if (pool->key != TLS_OUT_OF_INDEXES)
		{
			pool->stacksize = stacksize;
			pool->nthreads = 0;
			pool->tid = NULL;
			pool->terminating = 0;
			InitializeConditionVariable(&pool->term_cond);
			if (__thrdpool_create_threads(nthreads, pool) >= 0)
				return pool;

			TlsFree(pool->key);
		}
		else
			errno = ENOMEM;

		msgqueue_destroy(pool->msgqueue);
	}

	free(pool);
	return NULL;
}

void __thrdpool_schedule(const struct thrdpool_task *task, void *buf,
						 thrdpool_t *pool)
{
	((struct thrdpool_task_entry *)buf)->task = *task;
	msgqueue_put(buf, pool->msgqueue);
}

int thrdpool_schedule(const struct thrdpool_task *task, thrdpool_t *pool)
{
	void *buf;

	if (!task || !task->routine || !pool)
	{
		errno = EINVAL;
		return -1;
	}

	buf = malloc(sizeof (struct thrdpool_task_entry));
	if (buf)
	{
		((struct thrdpool_task_entry *)buf)->allocated = 1;
		__thrdpool_schedule(task, buf, pool);
		return 0;
	}

	return -1;
}

int thrdpool_schedule_preallocated(struct thrdpool_task_entry *entry,
								const struct thrdpool_task *task,
									thrdpool_t *pool)
{
	if (!entry || !task || !task->routine || !pool)
	{
		errno = EINVAL;
		return -1;
	}

	entry->allocated = 0;
	__thrdpool_schedule(task, entry, pool);
	return 0;
}

int thrdpool_in_pool(thrdpool_t *pool)
{
	return pool && pool->key != TLS_OUT_OF_INDEXES &&
		   TlsGetValue(pool->key) == pool;
}

int thrdpool_increase(thrdpool_t *pool)
{
	DWORD flags;
	uintptr_t thread;
	int error = 0;

	if (!pool)
	{
		errno = EINVAL;
		return -1;
	}

	/*
	 * Re-check under the pool lock: a concurrent thrdpool_destroy() may have
	 * terminated the pool between the caller's check and this call.  The
	 * upper layer must still serialize increase/destroy itself (see the
	 * contract in thrdpool.h) -- this is defense in depth, not a gate.
	 */
	flags = pool->stacksize ? STACK_SIZE_PARAM_IS_A_RESERVATION : 0;
	AcquireSRWLockExclusive(&pool->mutex);
	if (__thrdpool_termination(pool))
	{
		ReleaseSRWLockExclusive(&pool->mutex);
		errno = ECANCELED;
		return -1;
	}

	if (__thrdpool_thread_count(pool) == LONG_MAX)
	{
		ReleaseSRWLockExclusive(&pool->mutex);
		errno = EAGAIN;
		return -1;
	}

	/* Count first, then create; a failed create rolls back. */
	InterlockedIncrement(&pool->nthreads);
	thread = _beginthreadex(NULL, (unsigned)pool->stacksize,
							__thrdpool_routine, pool, flags, NULL);
	if (!thread)
	{
		/* The CRT is not guaranteed to set errno; expose a defined value. */
		error = errno;
		InterlockedDecrement(&pool->nthreads);
	}

	ReleaseSRWLockExclusive(&pool->mutex);
	if (thread)
	{
		CloseHandle((HANDLE)thread);
		return 0;
	}

	errno = error ? error : EAGAIN;
	return -1;
}

int thrdpool_decrease(thrdpool_t *pool)
{
	struct thrdpool_task_entry *entry;

	if (!pool)
	{
		errno = EINVAL;
		return -1;
	}

	entry = (struct thrdpool_task_entry *)malloc(sizeof (*entry));
	if (entry)
	{
		entry->allocated = 1;
		entry->task.routine = __thrdpool_exit_routine;
		entry->task.context = pool;
		msgqueue_put_head(entry, pool->msgqueue);
		return 0;
	}

	return -1;
}

void thrdpool_exit(thrdpool_t *pool)
{
	if (thrdpool_in_pool(pool))
		__thrdpool_exit_routine(pool);
}

void thrdpool_destroy(void (*pending)(const struct thrdpool_task *),
					  thrdpool_t *pool)
{
	int in_pool;
	struct thrdpool_task_entry *entry;

	if (!pool)
		return;

	in_pool = thrdpool_in_pool(pool);
	__thrdpool_terminate(in_pool, pool);
	while (1)
	{
		int allocated;
		entry = (struct thrdpool_task_entry *)msgqueue_get(pool->msgqueue);
		if (!entry)
			break;
		allocated = entry->allocated;

		if (pending && entry->task.routine != __thrdpool_exit_routine)
			pending(&entry->task);

		if (allocated)
			free(entry);
	}

	TlsFree(pool->key);
	pool->key = TLS_OUT_OF_INDEXES;
	msgqueue_destroy(pool->msgqueue);
	if (!in_pool)
		free(pool);
}

