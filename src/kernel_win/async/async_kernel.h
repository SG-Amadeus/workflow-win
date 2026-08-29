/*
  AsyncCore: Async kernel owned by Comm.

  This is the single container created by Comm that owns io_context, its
  run_one() threads and the operation pools. It is deliberately not called
  Runtime; it is the async kernel itself.
*/

#ifndef _ASYNC_ASYNC_KERNEL_H_
#define _ASYNC_ASYNC_KERNEL_H_

#include "io_context.h"
#include "op/op_pools.h"

class async_kernel
{
public:
	async_kernel();
	~async_kernel();

	async_kernel(const async_kernel &) = delete;
	async_kernel &operator=(const async_kernel &) = delete;

	int init(size_t io_threads);
	void deinit();
	int increase_worker();
	int decrease_worker();
	int is_worker_thread() const;

	io_context &get_io_context();

private:
	static unsigned __stdcall io_worker(void *ctx);
	int add_worker_locked();
	void reap_workers_locked();
	void destroy_workers();

	io_context io_;
	HANDLE *worker_threads_;
	size_t worker_size_;
	size_t worker_capacity_;
	op_pools *op_pools_;
	size_t worker_count_;
	bool initialized_;
	bool started_;
	bool work_owned_;
	CRITICAL_SECTION worker_mutex_;
};

#endif /* _ASYNC_ASYNC_KERNEL_H_ */

