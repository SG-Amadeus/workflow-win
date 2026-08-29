/*
  Copyright (c) 2020 Sogou, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

  Author: Wu Jiaxu (wujiaxu@sogou-inc.com)
*/

#include <stdio.h>
#include <string.h>
#ifdef _WIN32
#include <WinSock2.h>
#include <Windows.h>
#else
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#endif
#include <string>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <gtest/gtest.h>
#include "workflow/WFTaskFactory.h"

#define GET_CURRENT_MICRO	std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch()).count()

TEST(task_unittest, WFTimerTask)
{
	std::mutex mutex;
	std::condition_variable cond;
	bool done = false;
	auto *task = WFTaskFactory::create_timer_task(1000000, [&mutex, &cond, &done](WFTimerTask *task) {
		EXPECT_EQ(task->get_state(), WFT_STATE_SUCCESS);
		mutex.lock();
		done = true;
		mutex.unlock();
		cond.notify_one();
	});

	int64_t st = GET_CURRENT_MICRO;
	task->start();
	std::unique_lock<std::mutex> lock(mutex);
	while (!done)
		cond.wait(lock);

	lock.unlock();

	int64_t ed = GET_CURRENT_MICRO;
	EXPECT_LE(ed - st, 10000000) << "Timer Task too slow";
}

TEST(task_unittest, WFCounterTask1)
{
	std::mutex mutex;
	std::condition_variable cond;
	bool done = false;
	auto *task = WFTaskFactory::create_counter_task("abc", 2, [&mutex, &cond, &done](WFCounterTask *task) {
		auto state = task->get_state();

		EXPECT_EQ(state, WFT_STATE_SUCCESS);
		if (state == WFT_STATE_SUCCESS)
		{
			WFTaskFactory::count_by_name("abc", 0);
			task->count();
			WFTaskFactory::count_by_name("abc", 1);
		}

		mutex.lock();
		done = true;
		mutex.unlock();
		cond.notify_one();
	});

	task->start();
	for (int i = 0; i < 100; i++)
	{
		WFTaskFactory::count_by_name("abc");
		WFTaskFactory::count_by_name("abc");
	}

	std::unique_lock<std::mutex> lock(mutex);
	while (!done)
		cond.wait(lock);

	lock.unlock();
}

TEST(task_unittest, WFCounterTask2)
{
	std::mutex mutex;
	std::condition_variable cond;
	bool done = false;
	auto *task = WFTaskFactory::create_counter_task("def", 2, [&mutex, &cond, &done](WFCounterTask *task) {
		auto state = task->get_state();

		EXPECT_EQ(state, WFT_STATE_SUCCESS);
		if (state == WFT_STATE_SUCCESS)
		{
			WFTaskFactory::count_by_name("def", 0);
			task->count();
			WFTaskFactory::count_by_name("def", 1);
		}

		mutex.lock();
		done = true;
		mutex.unlock();
		cond.notify_one();
	});

	task->count();
	task->start();
	task->count();

	std::unique_lock<std::mutex> lock(mutex);
	while (!done)
		cond.wait(lock);

	lock.unlock();
}

TEST(task_unittest, WFGoTask)
{
	srand(time(NULL));
	std::mutex mutex;
	std::condition_variable cond;
	bool done = false;
	int target = rand() % 1024;
	int edit_inner = -1;

	auto&& f = [&mutex, &cond, &done, target, &edit_inner](int id) {
		EXPECT_EQ(target, id);
		edit_inner = 100;
		mutex.lock();
		done = true;
		mutex.unlock();
		cond.notify_one();
	};

	WFGoTask *task = WFTaskFactory::create_go_task("go", std::move(f), target);
	task->start();

	std::unique_lock<std::mutex> lock(mutex);
	while (!done)
		cond.wait(lock);

	lock.unlock();

	EXPECT_EQ(edit_inner, 100);
}

TEST(task_unittest, WFThreadTask)
{
	std::mutex mutex;
	std::condition_variable cond;
	bool done = false;

	using MyTaskIn = std::pair<int, int>;
	using MyTaskOut = int;
	using MyFactory = WFThreadTaskFactory<MyTaskIn, MyTaskOut>;
	using MyTask = WFThreadTask<MyTaskIn, MyTaskOut>;

	auto&& calc_multi = [](MyTaskIn *in, MyTaskOut *out) {
		*out = in->first * in->second;
	};

	auto *task = MyFactory::create_thread_task("calc", std::move(calc_multi), [&mutex, &cond, &done](MyTask *task) {
		auto state = task->get_state();

		EXPECT_EQ(state, WFT_STATE_SUCCESS);
		if (state == WFT_STATE_SUCCESS)
		{
			auto *in = task->get_input();
			auto *out = task->get_output();
			EXPECT_EQ(in->first * in->second, *out);
		}

		mutex.lock();
		done = true;
		mutex.unlock();
		cond.notify_one();
	});
	task->start();

	std::unique_lock<std::mutex> lock(mutex);
	while (!done)
		cond.wait(lock);

	lock.unlock();
}

TEST(task_unittest, WFFileIOTask)
{
	srand(time(NULL));
	std::mutex mutex;
	std::condition_variable cond;
	bool done = false;
	std::string file_path = "./" + std::to_string(time(NULL)) +
							"__" + std::to_string(rand() % 4096);

#ifdef _WIN32
	HANDLE file = CreateFileA(file_path.c_str(), GENERIC_READ | GENERIC_WRITE,
							  0, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL,
							  NULL);
	EXPECT_NE(file, INVALID_HANDLE_VALUE);
#define WFIO_FILE	file
#define WFIO_ARGS_FIELD	file
#else
	int fd = open(file_path.c_str(), O_RDWR | O_CREAT, 0644);
	EXPECT_TRUE(fd > 0);
#define WFIO_FILE	fd
#define WFIO_ARGS_FIELD	fd
#endif

	char writebuf[] = "testtest";
	char readbuf[16];

	auto *write = WFTaskFactory::create_pwrite_task(WFIO_FILE, writebuf, 8, 80,
	[WFIO_FILE](WFFileIOTask *task) {
		auto state = task->get_state();

		EXPECT_EQ(state, WFT_STATE_SUCCESS);
		if (state == WFT_STATE_SUCCESS)
		{
			auto *args = task->get_args();
			EXPECT_EQ(args->WFIO_ARGS_FIELD, WFIO_FILE);
			EXPECT_EQ(args->count, 8);
			EXPECT_EQ(args->offset, 80);
			EXPECT_TRUE(strncmp("testtest", (char *)args->buf, 8) == 0);
		}
	});

	auto *read = WFTaskFactory::create_pread_task(WFIO_FILE, readbuf, 8, 80,
	[WFIO_FILE](WFFileIOTask *task) {
		auto state = task->get_state();

		EXPECT_EQ(state, WFT_STATE_SUCCESS);
		if (state == WFT_STATE_SUCCESS)
		{
			auto *args = task->get_args();
			EXPECT_EQ(args->WFIO_ARGS_FIELD, WFIO_FILE);
			EXPECT_EQ(args->count, 8);
			EXPECT_EQ(args->offset, 80);
			EXPECT_TRUE(strncmp("testtest", (char *)args->buf, 8) == 0);
		}
	});

	auto *series = Workflow::create_series_work(write, [&mutex, &cond, &done](const SeriesWork *series) {
		mutex.lock();
		done = true;
		mutex.unlock();
		cond.notify_one();
	});

	series->push_back(read);
	series->start();
	std::unique_lock<std::mutex> lock(mutex);
	while (!done)
		cond.wait(lock);

	lock.unlock();

#ifdef _WIN32
	CloseHandle(file);
	DeleteFileA(file_path.c_str());
#else
	close(fd);
	remove(file_path.c_str());
#endif

#undef WFIO_FILE
#undef WFIO_ARGS_FIELD
}
TEST(task_unittest, WFFilePathIOTask)
{
	srand(time(NULL));
	std::mutex mutex;
	std::condition_variable cond;
	bool done = false;
	std::string file_path = "./" + std::to_string(time(NULL)) +
							"__" + std::to_string(rand() % 4096);

	char writebuf[] = "testtest";
	char readbuf[16];

#ifdef _WIN32
	HANDLE file = CreateFileA(file_path.c_str(), GENERIC_READ | GENERIC_WRITE,
							  0, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL,
							  NULL);
	EXPECT_NE(file, INVALID_HANDLE_VALUE);
	auto *write = WFTaskFactory::create_pwrite_task(file, writebuf, 8, 80,
	[](WFFileIOTask *task) {
		auto state = task->get_state();

		EXPECT_EQ(state, WFT_STATE_SUCCESS);
		if (state == WFT_STATE_SUCCESS)
		{
			auto *args = task->get_args();
			EXPECT_EQ(args->count, 8);
			EXPECT_EQ(args->offset, 80);
			EXPECT_TRUE(strncmp("testtest", (char *)args->buf, 8) == 0);
		}
	});

	auto *read = WFTaskFactory::create_pread_task(file, readbuf, 8, 80,
	[](WFFileIOTask *task) {
		auto state = task->get_state();

		EXPECT_EQ(state, WFT_STATE_SUCCESS);
		if (state == WFT_STATE_SUCCESS)
		{
			auto *args = task->get_args();
			EXPECT_EQ(args->count, 8);
			EXPECT_EQ(args->offset, 80);
			EXPECT_TRUE(strncmp("testtest", (char *)args->buf, 8) == 0);
		}
	});
#else
	auto *write = WFTaskFactory::create_pwrite_task(file_path, writebuf, 8, 80,
	[](WFFileIOTask *task) {
		auto state = task->get_state();

		EXPECT_EQ(state, WFT_STATE_SUCCESS);
		if (state == WFT_STATE_SUCCESS)
		{
			auto *args = task->get_args();
			EXPECT_EQ(args->count, 8);
			EXPECT_EQ(args->offset, 80);
			EXPECT_TRUE(strncmp("testtest", (char *)args->buf, 8) == 0);
		}
	});

	auto *read = WFTaskFactory::create_pread_task(file_path, readbuf, 8, 80,
	[](WFFileIOTask *task) {
		auto state = task->get_state();

		EXPECT_EQ(state, WFT_STATE_SUCCESS);
		if (state == WFT_STATE_SUCCESS)
		{
			auto *args = task->get_args();
			EXPECT_EQ(args->count, 8);
			EXPECT_EQ(args->offset, 80);
			EXPECT_TRUE(strncmp("testtest", (char *)args->buf, 8) == 0);
		}
	});
#endif

	auto *series = Workflow::create_series_work(write, [&mutex, &cond, &done](const SeriesWork *series) {
		mutex.lock();
		done = true;
		mutex.unlock();
		cond.notify_one();
	});

	series->push_back(read);
	series->start();
	std::unique_lock<std::mutex> lock(mutex);
	while (!done)
		cond.wait(lock);

	lock.unlock();

#ifdef _WIN32
	CloseHandle(file);
	DeleteFileA(file_path.c_str());
#else
	remove(file_path.c_str());
#endif
}
