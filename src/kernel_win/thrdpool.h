/*
  Copyright (c) 2019 Sogou, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

  Authors: Xie Han (xiehan@sogou-inc.com)
           Wu Jiaxu (wujiaxu@sogou-inc.com)
*/

#ifndef _THRDPOOL_H_
#define _THRDPOOL_H_

#include <stddef.h>

typedef struct __thrdpool thrdpool_t;

struct thrdpool_task
{
	void (*routine)(void *);
	void *context;
};

/* Caller-owned intrusive queue node. */
struct thrdpool_task_entry
{
	void *link;
	struct thrdpool_task task;
	int allocated;
};

#ifdef __cplusplus
extern "C"
{
#endif

/*
 * Concurrency contract: thrdpool_destroy() is a one-shot, exclusive
 * operation.  Once a thread begins destroy, no other thread may call any
 * thrdpool_*() function on the pool -- destroy, schedule, increase,
 * decrease, in_pool, and exit alike.  The owner layer must serialize the
 * whole pool lifecycle (typically with a higher-level lifecycle lock);
 * after destroy returns the pool object is freed, and any concurrent or
 * subsequent use is undefined behavior.  As defense in depth (not a
 * gate), thrdpool_increase() rejects a pool that has already entered
 * termination, and the destroy sequence itself never references the
 * terminator's own stack.
 *
 * Error strategy: every thrdpool_*() function reports failure through
 * errno and a NULL/-1 return -- this is the public error contract.  The
 * one exception is internal invariant corruption (a kernel call such as
 * DuplicateHandle, a join wait or TLS setup failing inside the worker
 * machinery), which raises RaiseFailFastException and terminates the
 * process rather than continue with corrupted concurrency state; see
 * thrdpool.cc for why there is no safe fallback in those paths.
 *
 * Porting differences from the Linux thrdpool are listed in thrdpool.cc;
 * notable ones are that thrdpool_create(0, ...) creates an empty pool
 * (filled later by increase), and that stacksize > UINT_MAX is rejected
 * with EINVAL because _beginthreadex takes the size as unsigned.
 */
thrdpool_t *thrdpool_create(size_t nthreads, size_t stacksize);
int thrdpool_schedule(const struct thrdpool_task *task, thrdpool_t *pool);
int thrdpool_schedule_preallocated(struct thrdpool_task_entry *entry,
								const struct thrdpool_task *task,
									thrdpool_t *pool);
int thrdpool_increase(thrdpool_t *pool);
int thrdpool_in_pool(thrdpool_t *pool);
int thrdpool_decrease(thrdpool_t *pool);
void thrdpool_exit(thrdpool_t *pool);
void thrdpool_destroy(void (*pending)(const struct thrdpool_task *),
					  thrdpool_t *pool);

#ifdef __cplusplus
}
#endif

#endif


