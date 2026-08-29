/*
 * Scheduler contract tests for the Windows kernel.
 *
 * These tests exercise the public scheduler surface: target/group
 * initialization, load-counter publication, and group add/remove/heap
 * growth. Permit acquisition/release is exercised indirectly by the upper
 * layers and by later integration tests.
 */

#include <errno.h>
#include <string.h>
#include <memory>
#include <vector>
#include <thread>
#include <chrono>
#include <mutex>
#include <functional>
#include <WinSock2.h>
#include <Windows.h>
#include <gtest/gtest.h>

#include "CommScheduler.h"

/* Test-only friend access to the private scheduler permit API. */
class SchedulerTestAccess
{
public:
	static CommTarget *acquire(CommSchedTarget *target, int wait_timeout)
	{
		return target->acquire(wait_timeout);
	}

	static void release(CommSchedTarget *target)
	{
		target->release();
	}

	static CommTarget *acquire(CommSchedGroup *group, int wait_timeout)
	{
		return group->acquire(wait_timeout);
	}

	static int wait_count(CommSchedTarget *target)
	{
		std::lock_guard<std::mutex> lock(target->mutex);
		return target->wait_cnt;
	}

	static int wait_count(CommSchedGroup *group)
	{
		std::lock_guard<std::mutex> lock(group->mutex);
		return group->wait_cnt;
	}
};

namespace {

struct sockaddr_in make_addr(unsigned short port = 1)
{
	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_port = htons(port);
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	return sin;
}

bool wait_for_waiter(const std::function<bool ()> &pred)
{
	auto deadline = std::chrono::steady_clock::now() +
					std::chrono::seconds(5);

	while (std::chrono::steady_clock::now() < deadline)
	{
		if (pred())
			return true;

		std::this_thread::yield();
	}

	return pred();
}

} /* anonymous namespace */

TEST(scheduler, target_init_rejects_zero_max_connections)
{
	struct sockaddr_in sin = make_addr();
	CommSchedTarget target;

	errno = 0;
	EXPECT_EQ(target.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000, 0), -1);
	EXPECT_EQ(errno, EINVAL);
}

TEST(scheduler, target_init_publishes_max_load)
{
	struct sockaddr_in sin = make_addr();
	CommSchedTarget target;

	ASSERT_EQ(target.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000, 7), 0);
	EXPECT_EQ(target.get_max_load(), 7u);
	EXPECT_EQ(target.get_cur_load(), 0u);
	target.deinit();
}

TEST(scheduler, target_rejects_invalid_address)
{
	CommSchedTarget target;

	errno = 0;
	EXPECT_EQ(target.init(NULL, 0, 1000, 1000, 1), -1);
	EXPECT_EQ(errno, EINVAL);
}

TEST(scheduler, group_add_remove_and_heap_growth)
{
	struct sockaddr_in sin = make_addr();
	CommSchedGroup group;
	std::vector<std::unique_ptr<CommSchedTarget>> targets;

	ASSERT_EQ(group.init(), 0);
	EXPECT_EQ(group.get_max_load(), 0u);
	EXPECT_EQ(group.get_cur_load(), 0u);

	for (int i = 0; i < 20; i++)
	{
		auto target = std::unique_ptr<CommSchedTarget>(
			new CommSchedTarget);
		ASSERT_EQ(target->init((const struct sockaddr *)&sin, sizeof sin,
							   1000, 1000, 1), 0);
		ASSERT_EQ(group.add(target.get()), 0);
		targets.push_back(std::move(target));
	}

	EXPECT_EQ(group.get_max_load(), 20u);
	EXPECT_EQ(group.get_cur_load(), 0u);

	for (auto &target : targets)
		EXPECT_EQ(group.remove(target.get()), 0);

	EXPECT_EQ(group.get_max_load(), 0u);
	EXPECT_EQ(group.get_cur_load(), 0u);
	group.deinit();

	for (auto &target : targets)
		target->deinit();
}

TEST(scheduler, group_tracks_sum_of_target_capacities)
{
	struct sockaddr_in sin = make_addr();
	CommSchedGroup group;
	CommSchedTarget t1;
	CommSchedTarget t2;
	CommSchedTarget t3;

	ASSERT_EQ(t1.init((const struct sockaddr *)&sin, sizeof sin,
					  1000, 1000, 2), 0);
	ASSERT_EQ(t2.init((const struct sockaddr *)&sin, sizeof sin,
					  1000, 1000, 3), 0);
	ASSERT_EQ(t3.init((const struct sockaddr *)&sin, sizeof sin,
					  1000, 1000, 5), 0);
	ASSERT_EQ(group.init(), 0);

	EXPECT_EQ(group.add(&t1), 0);
	EXPECT_EQ(group.get_max_load(), 2u);
	EXPECT_EQ(group.add(&t2), 0);
	EXPECT_EQ(group.get_max_load(), 5u);
	EXPECT_EQ(group.add(&t3), 0);
	EXPECT_EQ(group.get_max_load(), 10u);

	EXPECT_EQ(group.remove(&t2), 0);
	EXPECT_EQ(group.get_max_load(), 7u);
	EXPECT_EQ(group.remove(&t1), 0);
	EXPECT_EQ(group.remove(&t3), 0);
	EXPECT_EQ(group.get_max_load(), 0u);

	group.deinit();
	t1.deinit();
	t2.deinit();
	t3.deinit();
}

TEST(scheduler, group_duplicate_add_returns_eexist)
{
	struct sockaddr_in sin = make_addr();
	CommSchedGroup group;
	CommSchedTarget target;

	ASSERT_EQ(target.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000, 1), 0);
	ASSERT_EQ(group.init(), 0);
	ASSERT_EQ(group.add(&target), 0);

	errno = 0;
	EXPECT_EQ(group.add(&target), -1);
	EXPECT_EQ(errno, EEXIST);

	EXPECT_EQ(group.remove(&target), 0);
	group.deinit();
	target.deinit();
}

TEST(scheduler, group_remove_foreign_target_returns_enoent)
{
	struct sockaddr_in sin = make_addr();
	CommSchedGroup group;
	CommSchedTarget target;

	ASSERT_EQ(target.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000, 1), 0);
	ASSERT_EQ(group.init(), 0);

	errno = 0;
	EXPECT_EQ(group.remove(&target), -1);
	EXPECT_EQ(errno, ENOENT);

	group.deinit();
	target.deinit();
}

TEST(scheduler, target_acquire_release_accounting)
{
	struct sockaddr_in sin = make_addr();
	CommSchedTarget target;

	ASSERT_EQ(target.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000, 2), 0);
	EXPECT_EQ(target.get_max_load(), 2u);
	EXPECT_EQ(target.get_cur_load(), 0u);

	EXPECT_EQ(SchedulerTestAccess::acquire(&target, 0), &target);
	EXPECT_EQ(target.get_cur_load(), 1u);
	EXPECT_EQ(SchedulerTestAccess::acquire(&target, 0), &target);
	EXPECT_EQ(target.get_cur_load(), 2u);

	errno = 0;
	EXPECT_EQ(SchedulerTestAccess::acquire(&target, 0), nullptr);
	EXPECT_EQ(errno, EAGAIN);
	EXPECT_EQ(target.get_cur_load(), 2u);

	SchedulerTestAccess::release(&target);
	EXPECT_EQ(target.get_cur_load(), 1u);
	EXPECT_EQ(SchedulerTestAccess::acquire(&target, 0), &target);
	EXPECT_EQ(target.get_cur_load(), 2u);

	SchedulerTestAccess::release(&target);
	SchedulerTestAccess::release(&target);
	EXPECT_EQ(target.get_cur_load(), 0u);
	target.deinit();
}

TEST(scheduler, target_wait_timeout_returns_etimedout)
{
	struct sockaddr_in sin = make_addr();
	CommSchedTarget target;

	ASSERT_EQ(target.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000, 1), 0);
	ASSERT_EQ(SchedulerTestAccess::acquire(&target, 0), &target);

	errno = 0;
	EXPECT_EQ(SchedulerTestAccess::acquire(&target, 1), nullptr);
	EXPECT_EQ(errno, ETIMEDOUT);
	EXPECT_EQ(target.get_cur_load(), 1u);

	SchedulerTestAccess::release(&target);
	target.deinit();
}

TEST(scheduler, group_acquire_release_accounting)
{
	struct sockaddr_in sin = make_addr();
	CommSchedGroup group;
	CommSchedTarget t1;
	CommSchedTarget t2;

	ASSERT_EQ(t1.init((const struct sockaddr *)&sin, sizeof sin,
					  1000, 1000, 1), 0);
	ASSERT_EQ(t2.init((const struct sockaddr *)&sin, sizeof sin,
					  1000, 1000, 1), 0);
	ASSERT_EQ(group.init(), 0);
	ASSERT_EQ(group.add(&t1), 0);
	ASSERT_EQ(group.add(&t2), 0);
	EXPECT_EQ(group.get_max_load(), 2u);

	CommTarget *first = SchedulerTestAccess::acquire(&group, 0);
	ASSERT_TRUE(first != nullptr);
	EXPECT_EQ(group.get_cur_load(), 1u);

	CommTarget *second = SchedulerTestAccess::acquire(&group, 0);
	ASSERT_TRUE(second != nullptr);
	EXPECT_EQ(group.get_cur_load(), 2u);

	errno = 0;
	EXPECT_EQ(SchedulerTestAccess::acquire(&group, 0), nullptr);
	EXPECT_EQ(errno, EAGAIN);
	EXPECT_EQ(group.get_cur_load(), 2u);

	SchedulerTestAccess::release(static_cast<CommSchedTarget *>(first));
	SchedulerTestAccess::release(static_cast<CommSchedTarget *>(second));
	EXPECT_EQ(group.get_cur_load(), 0u);

	EXPECT_EQ(group.remove(&t1), 0);
	EXPECT_EQ(group.remove(&t2), 0);
	group.deinit();
	t1.deinit();
	t2.deinit();
}

TEST(scheduler, group_acquire_waiter_release_wakes_one)
{
	struct sockaddr_in sin = make_addr();
	CommSchedGroup group;
	CommSchedTarget t1;

	ASSERT_EQ(t1.init((const struct sockaddr *)&sin, sizeof sin,
					  1000, 1000, 1), 0);
	ASSERT_EQ(group.init(), 0);
	ASSERT_EQ(group.add(&t1), 0);
	EXPECT_EQ(group.get_max_load(), 1u);

	CommTarget *first = SchedulerTestAccess::acquire(&group, 0);
	ASSERT_TRUE(first != nullptr);
	EXPECT_EQ(group.get_cur_load(), 1u);

	std::thread waiter([&] {
		CommTarget *got = SchedulerTestAccess::acquire(&group, -1);
		ASSERT_TRUE(got != nullptr);
		SchedulerTestAccess::release(static_cast<CommSchedTarget *>(got));
	});

	bool waiting = wait_for_waiter([&] {
		return SchedulerTestAccess::wait_count(&group) == 1;
	});

	SchedulerTestAccess::release(static_cast<CommSchedTarget *>(first));

	waiter.join();
	EXPECT_TRUE(waiting);
	EXPECT_EQ(group.get_cur_load(), 0u);

	EXPECT_EQ(group.remove(&t1), 0);
	group.deinit();
	t1.deinit();
}

TEST(scheduler, target_acquire_waiter_release_wakes_one)
{
	struct sockaddr_in sin = make_addr();
	CommSchedTarget target;

	ASSERT_EQ(target.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000, 1), 0);
	CommTarget *first = SchedulerTestAccess::acquire(&target, 0);
	ASSERT_TRUE(first != nullptr);
	EXPECT_EQ(target.get_cur_load(), 1u);

	std::thread waiter([&] {
		CommTarget *got = SchedulerTestAccess::acquire(&target, -1);
		ASSERT_TRUE(got != nullptr);
		SchedulerTestAccess::release(static_cast<CommSchedTarget *>(got));
	});

	bool waiting = wait_for_waiter([&] {
		return SchedulerTestAccess::wait_count(&target) == 1;
	});

	SchedulerTestAccess::release(static_cast<CommSchedTarget *>(first));

	waiter.join();
	EXPECT_TRUE(waiting);
	EXPECT_EQ(target.get_cur_load(), 0u);
	target.deinit();
}
