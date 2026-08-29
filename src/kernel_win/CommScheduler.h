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

#ifndef _COMMSCHEDULER_H_
#define _COMMSCHEDULER_H_

#include <openssl/ssl.h>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include "PlatformSocket.h"
#include "Communicator.h"

class CommSchedObject
{
public:
	size_t get_max_load() const
	{
		return this->max_load_pub.load(std::memory_order_relaxed);
	}

	size_t get_cur_load() const
	{
		return this->cur_load_pub.load(std::memory_order_relaxed);
	}

private:
	virtual CommTarget *acquire(int wait_timeout) = 0;
	friend class SchedulerTestAccess;

protected:
	/* Lock-internal counters. All scheduler mutations happen under the
	 * appropriate target/group mutex, so these remain plain fields. */
	size_t max_load = 0;
	size_t cur_load = 0;

	/* Published atomic snapshots for lock-free get_*_load() calls. */
	std::atomic<size_t> max_load_pub{0};
	std::atomic<size_t> cur_load_pub{0};

	void set_max_load(size_t load)
	{
		this->max_load = load;
		this->max_load_pub.store(load, std::memory_order_relaxed);
	}

	void set_cur_load(size_t load)
	{
		this->cur_load = load;
		this->cur_load_pub.store(load, std::memory_order_relaxed);
	}

	void add_max_load(size_t load)
	{
		this->max_load += load;
		this->max_load_pub.store(this->max_load, std::memory_order_relaxed);
	}

	void sub_max_load(size_t load)
	{
		this->max_load -= load;
		this->max_load_pub.store(this->max_load, std::memory_order_relaxed);
	}

	void inc_cur_load()
	{
		++this->cur_load;
		this->cur_load_pub.store(this->cur_load, std::memory_order_relaxed);
	}

	void dec_cur_load()
	{
		--this->cur_load;
		this->cur_load_pub.store(this->cur_load, std::memory_order_relaxed);
	}

	void add_cur_load(size_t load)
	{
		this->cur_load += load;
		this->cur_load_pub.store(this->cur_load, std::memory_order_relaxed);
	}

	void sub_cur_load(size_t load)
	{
		this->cur_load -= load;
		this->cur_load_pub.store(this->cur_load, std::memory_order_relaxed);
	}

public:
	virtual ~CommSchedObject() { }
	friend class CommScheduler;
};

class CommSchedGroup;

class CommSchedTarget : public CommSchedObject, public CommTarget
{
public:
	int init(const struct sockaddr *addr, socklen_t addrlen,
			 int connect_timeout, int response_timeout,
			 size_t max_connections);
	void deinit();

public:
	int init(const struct sockaddr *addr, socklen_t addrlen, SSL_CTX *ssl_ctx,
			 int connect_timeout, int ssl_connect_timeout, int response_timeout,
			 size_t max_connections)
	{
		int ret = this->init(addr, addrlen, connect_timeout, response_timeout,
							 max_connections);

		if (ret >= 0)
			this->set_ssl(ssl_ctx, ssl_connect_timeout);

		return ret;
	}

private:
	virtual CommTarget *acquire(int wait_timeout); /* final */
	virtual void release(); /* final */

private:
	CommSchedGroup *group;
	int index;
	int wait_cnt;
	std::mutex mutex;
	std::condition_variable cond;
	friend class CommSchedGroup;
	friend class SchedulerTestAccess;
};

class CommSchedGroup : public CommSchedObject
{
public:
	int init();
	void deinit();
	int add(CommSchedTarget *target);
	int remove(CommSchedTarget *target);

private:
	virtual CommTarget *acquire(int wait_timeout); /* final */

private:
	CommSchedTarget **tg_heap;
	int heap_size;
	int heap_buf_size;
	int wait_cnt;
	std::mutex mutex;
	std::condition_variable cond;

private:
	static int target_cmp(CommSchedTarget *target1, CommSchedTarget *target2);
	void heapify(int top);
	void heap_adjust(int index, int swap_on_equal);
	int heap_insert(CommSchedTarget *target);
	void heap_remove(int index);
	friend class CommSchedTarget;
	friend class SchedulerTestAccess;
};

class CommScheduler
{
public:
	int init(size_t poller_threads, size_t handler_threads)
	{
		return this->comm.init(poller_threads, handler_threads);
	}

	void deinit()
	{
		this->comm.deinit();
	}

	/* wait_timeout in milliseconds, -1 for no timeout. */
	int request(CommSession *session, CommSchedObject *object,
				int wait_timeout, CommTarget **target)
	{
		int ret = -1;

		*target = object->acquire(wait_timeout);
		if (*target)
		{
			ret = this->comm.request(session, *target);
			if (ret < 0)
				(*target)->release();
		}

		return ret;
	}

	/* for services. */
	int reply(CommSession *session)
	{
		return this->comm.reply(session);
	}

	int shutdown(CommSession *session)
	{
		return this->comm.shutdown(session);
	}

	int push(const void *buf, size_t size, CommSession *session)
	{
		return this->comm.push(buf, size, session);
	}

	int bind(CommService *service)
	{
		return this->comm.bind(service);
	}

	void unbind(CommService *service)
	{
		this->comm.unbind(service);
	}

	/* for sleepers. */
	int sleep(SleepSession *session)
	{
		return this->comm.sleep(session);
	}

	/* Call 'unsleep' only before 'handle()' returns. */
	int unsleep(SleepSession *session)
	{
		return this->comm.unsleep(session);
	}

	/* for file I/O services. */
	int io_bind(IOService *service)
	{
		return this->comm.io_bind(service);
	}

	void io_unbind(IOService *service)
	{
		this->comm.io_unbind(service);
	}
//#endif
public:
	int is_handler_thread() const
	{
		return this->comm.is_handler_thread();
	}

	int increase_handler_thread()
	{
		return this->comm.increase_handler_thread();
	}

	int decrease_handler_thread()
	{
		return this->comm.decrease_handler_thread();
	}

	void customize_event_handler(CommEventHandler *handler)
	{
		this->comm.customize_event_handler(handler);
	}

private:
	Communicator comm;

public:
	virtual ~CommScheduler() { }
};

#endif


