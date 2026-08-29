/*
  Performance regression unit tests for AsyncCore.

  These are deliberately not micro-optimized tests; they measure end-to-end
  throughput of the public async modules and compare them with the ASIO
  implementations that AsyncCore is intended to replace.  The assertions are
  intentionally loose sanity bounds to catch catastrophic regressions;
  detailed numbers are printed for manual comparison.

  Current modules under measurement:
    - workflow thrdpool (raw baseline)
    - async/io_context      vs ASIO io_context
    - async/strand    vs ASIO strand
*/

#include "async/io_context.h"
#include "async/strand.h"
#include "thrdpool.h"

#include <asio/io_context.hpp>
#include <asio/post.hpp>
#include <asio/strand.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

namespace
{

struct count_wait
{
	std::mutex mutex;
	std::condition_variable cond;
	int remaining = 0;

	void set(int n)
	{
		std::lock_guard<std::mutex> lock(mutex);
		remaining = n;
	}

	void count_down()
	{
		bool zero = false;

		{
			std::lock_guard<std::mutex> lock(mutex);
			if (remaining > 0)
				--remaining;
			zero = remaining == 0;
		}

		if (zero)
			cond.notify_all();
	}

	bool wait(int timeout_ms = 30000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		return cond.wait_for(lock, std::chrono::milliseconds(timeout_ms),
							 [this]() { return remaining == 0; });
	}
};

static void count_down_fn(void *arg)
{
	static_cast<count_wait *>(arg)->count_down();
}

static void thrdpool_count_down_fn(void *arg)
{
	static_cast<count_wait *>(arg)->count_down();
}

static double ms_since(const std::chrono::steady_clock::time_point &start)
{
	return std::chrono::duration<double, std::milli>(
		std::chrono::steady_clock::now() - start).count();
}

static double ops_per_sec(int n, double ms)
{
	return ms > 0.0 ? n * 1000.0 / ms : 0.0;
}

double bench_async_io_context(int n, int threads)
{
	struct async_io_context *io_context = async_io_context_create(threads);
	count_wait wait;
	double ms;
	auto start = std::chrono::steady_clock::now();

	if (!io_context)
		return 0.0;

	if (async_io_context_start(io_context) != 0)
	{
		async_io_context_destroy(io_context);
		return 0.0;
	}

	wait.set(n);
	for (int i = 0; i < n; ++i)
	{
		if (async_io_context_post(io_context, count_down_fn, &wait) != 0)
		{
			async_io_context_destroy(io_context);
			return 0.0;
		}
	}

	if (!wait.wait())
	{
		async_io_context_destroy(io_context);
		return 0.0;
	}

	ms = ms_since(start);
	async_io_context_destroy(io_context);
	return ops_per_sec(n, ms);
}

double bench_asio_io_context(int n, int threads)
{
	asio::io_context io;
	auto work = asio::make_work_guard(io);
	std::vector<std::thread> pool;
	count_wait wait;
	double ms;
	auto start = std::chrono::steady_clock::now();

	for (int i = 0; i < threads; ++i)
		pool.emplace_back([&io]() { io.run(); });

	wait.set(n);
	for (int i = 0; i < n; ++i)
		asio::post(io, [&wait]() { wait.count_down(); });

	work.reset();
	for (std::thread &t : pool)
		t.join();

	ms = ms_since(start);
	return ops_per_sec(n, ms);
}

double bench_strand(int n, int threads)
{
	struct async_io_context *io_context = async_io_context_create(threads);
	strand_t *strand;
	count_wait wait;
	double ms;
	auto start = std::chrono::steady_clock::now();

	if (!io_context)
		return 0.0;

	if (async_io_context_start(io_context) != 0)
	{
		async_io_context_destroy(io_context);
		return 0.0;
	}

	strand = strand_create(io_context);
	if (!strand)
	{
		async_io_context_destroy(io_context);
		return 0.0;
	}

	wait.set(n);
	for (int i = 0; i < n; ++i)
	{
		if (strand_post(strand, count_down_fn, &wait) != 0)
		{
			strand_destroy(strand);
			async_io_context_destroy(io_context);
			return 0.0;
		}
	}

	if (!wait.wait())
	{
		strand_destroy(strand);
		async_io_context_destroy(io_context);
		return 0.0;
	}

	ms = ms_since(start);
	strand_destroy(strand);
	async_io_context_destroy(io_context);
	return ops_per_sec(n, ms);
}

double bench_asio_strand(int n, int threads)
{
	asio::io_context io;
	auto work = asio::make_work_guard(io);
	auto strand = asio::make_strand(io);
	std::vector<std::thread> pool;
	count_wait wait;
	double ms;
	auto start = std::chrono::steady_clock::now();

	for (int i = 0; i < threads; ++i)
		pool.emplace_back([&io]() { io.run(); });

	wait.set(n);
	for (int i = 0; i < n; ++i)
		asio::post(strand, [&wait]() { wait.count_down(); });

	work.reset();
	for (std::thread &t : pool)
		t.join();

	ms = ms_since(start);
	return ops_per_sec(n, ms);
}

double bench_thrdpool(int n, int threads)
{
	thrdpool_t *pool = thrdpool_create(threads, 0);
	count_wait wait;
	double ms;
	auto start = std::chrono::steady_clock::now();

	if (!pool)
		return 0.0;

	wait.set(n);

	for (int i = 0; i < n; ++i)
	{
		struct thrdpool_task task;

		task.routine = thrdpool_count_down_fn;
		task.context = &wait;
		if (thrdpool_schedule(&task, pool) != 0)
		{
			thrdpool_destroy(NULL, pool);
			return 0.0;
		}
	}

	if (!wait.wait())
	{
		thrdpool_destroy(NULL, pool);
		return 0.0;
	}

	ms = ms_since(start);
	thrdpool_destroy(NULL, pool);
	return ops_per_sec(n, ms);
}

void print_result(const char *name, double ops)
{
	std::cout << "[ PERF ] " << name << ": " << static_cast<long long>(ops)
			  << " ops/s" << std::endl;
}

} /* namespace */

TEST(async_core_perf, thrdpool_baseline)
{
	const int n = 100000;
	const int threads = 4;
	double raw_pool = bench_thrdpool(n, threads);

	print_result("thrdpool", raw_pool);
	EXPECT_GT(raw_pool, 0.0);
	EXPECT_GT(raw_pool, 10000.0);
}

TEST(async_core_perf, io_context_vs_asio_io_context)
{
	const int n = 100000;
	const int threads = 4;
	double async_io_context = bench_async_io_context(n, threads);
	double asio_io = bench_asio_io_context(n, threads);

	print_result("async_io_context", async_io_context);
	print_result("asio_io_context", asio_io);
	if (asio_io > 0.0)
		std::cout << "[ PERF ] ratio async/asio: " << async_io_context / asio_io << std::endl;

	EXPECT_GT(async_io_context, 0.0);
	EXPECT_GT(asio_io, 0.0);
	EXPECT_GT(async_io_context, 10000.0);
}

TEST(async_core_perf, strand_vs_asio_strand)
{
	const int n = 100000;
	const int threads = 4;
	double strand = bench_strand(n, threads);
	double asio_strand = bench_asio_strand(n, threads);

	print_result("strand", strand);
	print_result("asio_strand", asio_strand);
	if (asio_strand > 0.0)
		std::cout << "[ PERF ] ratio async/asio: " << strand / asio_strand << std::endl;

	EXPECT_GT(strand, 0.0);
	EXPECT_GT(asio_strand, 0.0);
	EXPECT_GT(strand, 10000.0);
}
