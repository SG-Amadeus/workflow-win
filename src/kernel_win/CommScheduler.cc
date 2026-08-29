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

#include <errno.h>
#include <assert.h>
#include <stdlib.h>
#include <chrono>

#if defined(_MSC_VER) && (defined(_M_X64) || defined(_M_ARM64))
#include <intrin.h>
#endif

#include "PlatformSocket.h"
#include "CommScheduler.h"

namespace {

template<class Predicate>
inline bool wait_condition(std::condition_variable &cond,
						   std::unique_lock<std::mutex> &lock,
						   int wait_timeout,
						   Predicate pred)
{
	if (wait_timeout < 0)
	{
		cond.wait(lock, pred);
		return true;
	}

	return cond.wait_for(lock, std::chrono::milliseconds(wait_timeout), pred);
}

inline int compare_load_product(size_t a, size_t b,
								size_t c, size_t d)
{
#if defined(_MSC_VER) && (defined(_M_X64) || defined(_M_ARM64))
	unsigned __int64 hi1, lo1, hi2, lo2;

	lo1 = _umul128((unsigned __int64)a, (unsigned __int64)b, &hi1);
	lo2 = _umul128((unsigned __int64)c, (unsigned __int64)d, &hi2);
	if (hi1 != hi2)
		return hi1 < hi2 ? -1 : 1;
	if (lo1 != lo2)
		return lo1 < lo2 ? -1 : 1;
	return 0;
#elif defined(__SIZEOF_INT128__)
	unsigned __int128 p1 = (unsigned __int128)a * b;
	unsigned __int128 p2 = (unsigned __int128)c * d;

	if (p1 < p2)
		return -1;
	else if (p1 > p2)
		return 1;
	else
		return 0;
#else
	/* This fallback is for 32-bit platforms where size_t is 32-bit and the
	 * 64-bit product is exact. On 64-bit MSVC/GCC/Clang the branches above
	 * are used. */
	unsigned long long p1 = (unsigned long long)a * b;
	unsigned long long p2 = (unsigned long long)c * d;

	if (p1 < p2)
		return -1;
	else if (p1 > p2)
		return 1;
	else
		return 0;
#endif
}

} /* anonymous namespace */

int CommSchedTarget::init(const struct sockaddr *addr, socklen_t addrlen,
						  int connect_timeout, int response_timeout,
						  size_t max_connections)
{
	if (max_connections == 0)
	{
		errno = EINVAL;
		return -1;
	}

	if (this->CommTarget::init(addr, addrlen, connect_timeout,
							   response_timeout) >= 0)
	{
		this->set_max_load(max_connections);
		this->set_cur_load(0);
		this->wait_cnt = 0;
		this->group = NULL;
		return 0;
	}

	return -1;
}

void CommSchedTarget::deinit()
{
	assert(this->group == NULL);
	assert(this->wait_cnt == 0);
	assert(this->cur_load == 0);

	this->CommTarget::deinit();
}

CommTarget *CommSchedTarget::acquire(int wait_timeout)
{
	int ret;
	auto&& pred = [this] { return this->cur_load < this->max_load; };
	std::unique_lock<std::mutex> lock(this->mutex);

	if (this->group)
	{
		std::unique_lock<std::mutex> group_lock(this->group->mutex);

		lock.swap(group_lock);
	}

	if (this->cur_load >= this->max_load)
	{
		if (wait_timeout != 0)
		{
			this->wait_cnt++;
			if (!wait_condition(this->cond, lock, wait_timeout, pred))
				ret = ETIMEDOUT;

			this->wait_cnt--;
		}
		else
			ret = EAGAIN;
	}

	if (this->cur_load < this->max_load)
	{
		this->inc_cur_load();
		if (this->group)
		{
			this->group->inc_cur_load();
			this->group->heapify(this->index);
		}

		ret = 0;
	}

	lock.unlock();
	if (ret)
	{
		errno = ret;
		return NULL;
	}

	return this;
}

void CommSchedTarget::release()
{
	std::unique_lock<std::mutex> lock(this->mutex);

	if (this->group)
	{
		std::unique_lock<std::mutex> group_lock(this->group->mutex);

		lock.swap(group_lock);
	}

	this->dec_cur_load();

	/* A release frees exactly one slot. The wake-up policy is explicit:
	 * target waiters have strict priority over group waiters, so a slot freed
	 * on this target is first offered to waiters on this exact target. Only
	 * when no target waiter exists is the slot offered to a group waiter. */
	if (this->wait_cnt > 0)
		this->cond.notify_one();

	if (this->group)
	{
		this->group->dec_cur_load();
		if (this->wait_cnt == 0 && this->group->wait_cnt > 0)
			this->group->cond.notify_one();

		this->group->heap_adjust(this->index, this->has_idle_conn());
	}

	lock.unlock();
}

int CommSchedGroup::target_cmp(CommSchedTarget *target1,
							   CommSchedTarget *target2)
{
	size_t load1 = target1->cur_load;
	size_t load2 = target2->cur_load;
	size_t cap1 = target1->max_load;
	size_t cap2 = target2->max_load;

	return compare_load_product(load1, cap2, load2, cap1);
}

void CommSchedGroup::heap_adjust(int index, int swap_on_equal)
{
	CommSchedTarget *target = this->tg_heap[index];
	CommSchedTarget *parent;

	while (index > 0)
	{
		parent = this->tg_heap[(index - 1) / 2];
		if (CommSchedGroup::target_cmp(target, parent) < swap_on_equal)
		{
			this->tg_heap[index] = parent;
			parent->index = index;
			index = (index - 1) / 2;
		}
		else
			break;
	}

	this->tg_heap[index] = target;
	target->index = index;
}

/* Fastest heapify ever. */
void CommSchedGroup::heapify(int top)
{
	CommSchedTarget *target = this->tg_heap[top];
	int last = this->heap_size - 1;
	CommSchedTarget **child;
	int i;

	while (i = 2 * top + 1, i < last)
	{
		child = &this->tg_heap[i];
		if (CommSchedGroup::target_cmp(child[0], target) < 0)
		{
			if (CommSchedGroup::target_cmp(child[1], child[0]) < 0)
			{
				this->tg_heap[top] = child[1];
				child[1]->index = top;
				top = i + 1;
			}
			else
			{
				this->tg_heap[top] = child[0];
				child[0]->index = top;
				top = i;
			}
		}
		else
		{
			if (CommSchedGroup::target_cmp(child[1], target) < 0)
			{
				this->tg_heap[top] = child[1];
				child[1]->index = top;
				top = i + 1;
			}
			else
			{
				this->tg_heap[top] = target;
				target->index = top;
				return;
			}
		}
	}

	if (i == last)
	{
		child = &this->tg_heap[i];
		if (CommSchedGroup::target_cmp(child[0], target) < 0)
		{
			this->tg_heap[top] = child[0];
			child[0]->index = top;
			top = i;
		}
	}

	this->tg_heap[top] = target;
	target->index = top;
}

int CommSchedGroup::heap_insert(CommSchedTarget *target)
{
	if (this->heap_size == this->heap_buf_size)
	{
		int new_size = 2 * this->heap_buf_size;

		if (new_size < this->heap_buf_size)
		{
			errno = ENOMEM;
			return -1;
		}

		void *new_base = realloc(this->tg_heap,
								 new_size * sizeof (void *));
		if (new_base)
		{
			this->tg_heap = (CommSchedTarget **)new_base;
			this->heap_buf_size = new_size;
		}
		else
		{
			errno = ENOMEM;
			return -1;
		}
	}

	this->tg_heap[this->heap_size] = target;
	target->index = this->heap_size;
	this->heap_adjust(this->heap_size, 0);
	this->heap_size++;
	return 0;
}

void CommSchedGroup::heap_remove(int index)
{
	CommSchedTarget *target;

	this->heap_size--;
	if (index != this->heap_size)
	{
		target = this->tg_heap[this->heap_size];
		this->tg_heap[index] = target;
		target->index = index;
		this->heap_adjust(index, 0);
		this->heapify(target->index);
	}
}

#define COMMGROUP_INIT_SIZE		4

int CommSchedGroup::init()
{
	this->tg_heap = (CommSchedTarget **)malloc(COMMGROUP_INIT_SIZE *
											  sizeof (void *));
	if (!this->tg_heap)
	{
		errno = ENOMEM;
		return -1;
	}

	this->heap_buf_size = COMMGROUP_INIT_SIZE;
	this->heap_size = 0;
	this->set_max_load(0);
	this->set_cur_load(0);
	this->wait_cnt = 0;
	return 0;
}

void CommSchedGroup::deinit()
{
	assert(this->heap_size == 0);
	assert(this->wait_cnt == 0);
	assert(this->cur_load == 0);
	assert(this->max_load == 0);

	free(this->tg_heap);
	this->tg_heap = NULL;
}

int CommSchedGroup::add(CommSchedTarget *target)
{
	int ret = -1;
	std::lock(target->mutex, this->mutex);
	std::lock_guard<std::mutex> lock1(target->mutex, std::adopt_lock);
	std::lock_guard<std::mutex> lock2(this->mutex, std::adopt_lock);

	if (target->group == NULL && target->wait_cnt == 0)
	{
		if (this->heap_insert(target) >= 0)
		{
			target->group = this;
			this->add_max_load(target->max_load);
			this->add_cur_load(target->cur_load);
			if (this->wait_cnt > 0 && this->cur_load < this->max_load)
				this->cond.notify_one();

			ret = 0;
		}
		else
			errno = ENOMEM;
	}
	else if (target->group == this)
		errno = EEXIST;
	else if (target->group)
		errno = EINVAL;
	else
		errno = EBUSY;

	return ret;
}

int CommSchedGroup::remove(CommSchedTarget *target)
{
	int ret = -1;
	std::lock(target->mutex, this->mutex);
	std::lock_guard<std::mutex> lock1(target->mutex, std::adopt_lock);
	std::lock_guard<std::mutex> lock2(this->mutex, std::adopt_lock);

	if (target->group == this && target->wait_cnt == 0)
	{
		this->heap_remove(target->index);
		this->sub_max_load(target->max_load);
		this->sub_cur_load(target->cur_load);
		target->group = NULL;
		ret = 0;
	}
	else if (target->group != this)
		errno = ENOENT;
	else
		errno = EBUSY;

	return ret;
}

CommTarget *CommSchedGroup::acquire(int wait_timeout)
{
	CommSchedTarget *target;
	int ret;
	auto&& pred = [this] { return this->cur_load < this->max_load; };
	std::unique_lock<std::mutex> lock(this->mutex);

	if (this->cur_load >= this->max_load)
	{
		if (wait_timeout != 0)
		{
			this->wait_cnt++;
			if (!wait_condition(this->cond, lock, wait_timeout, pred))
				ret = ETIMEDOUT;

			this->wait_cnt--;
		}
		else
			ret = EAGAIN;
	}

	if (this->cur_load < this->max_load)
	{
		target = this->tg_heap[0];
		target->inc_cur_load();
		this->inc_cur_load();
		this->heapify(0);
		ret = 0;
	}

	lock.unlock();
	if (ret)
	{
		errno = ret;
		return NULL;
	}

	return target;
}


