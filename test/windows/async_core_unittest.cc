/*
  Unit tests for the streamlined AsyncCore kernel.

  Covered modules:
    - async/op/cancellation
    - async/win_iocp_operation
    - async/io_context
    - async/strand
    - async/executor
    - async/executor_work_guard
    - async/steady_timer
    - async/random_access_handle
    - async/tcp_socket
    - async/tcp_acceptor
    - async/udp_socket
    - async/ssl_stream

  The kernel is intentionally non-template and follows the Workflow style:
  intrusive list.h queues, C function pointer callbacks, void *context,
  errno-based error reporting, and op self-destruction.
*/

#include "async/op/cancellation.h"
#include "async/error.h"
#include "async/executor.h"
#include "async/io_context.h"
#include "async/random_access_handle.h"
#include "async/op/message.h"
#include "async/steady_timer.h"
#include "async/ssl_stream.h"
#include "async/strand.h"
#include "async/tcp_acceptor.h"
#include "async/tcp_socket.h"
#include "async/udp_socket.h"

#include <openssl/ssl.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace
{

class WSAEnv
{
public:
	WSAEnv()
	{
		WSADATA data;
		WSAStartup(MAKEWORD(2, 2), &data);
	}

	~WSAEnv()
	{
		WSACleanup();
	}
};

static WSAEnv g_wsa_env;

static int error_errno(const async_error_code &error)
{
	return async_error_to_errno(error);
}

static std::string find_repo_root()
{
	char buf[MAX_PATH];
	GetCurrentDirectoryA(MAX_PATH, buf);
	std::string dir = buf;

	for (;;)
	{
		std::string cert = dir + "\\server.crt";
		if (GetFileAttributesA(cert.c_str()) != INVALID_FILE_ATTRIBUTES)
			return dir;

		size_t pos = dir.find_last_of('\\');
		if (pos == std::string::npos)
			break;
		dir = dir.substr(0, pos);
	}

	return "";
}

static SSL_CTX *make_ssl_ctx(int server)
{
	SSL_CTX *ctx = SSL_CTX_new(server ? TLS_server_method()
									  : TLS_client_method());
	if (!ctx)
		return nullptr;

	if (server)
	{
		std::string root = find_repo_root();
		if (root.empty())
		{
			SSL_CTX_free(ctx);
			return nullptr;
		}

		std::string cert = root + "\\server.crt";
		std::string key = root + "\\server.key";
		if (SSL_CTX_use_certificate_chain_file(ctx, cert.c_str()) != 1 ||
			SSL_CTX_use_PrivateKey_file(ctx, key.c_str(), SSL_FILETYPE_PEM) != 1)
		{
			SSL_CTX_free(ctx);
			return nullptr;
		}
	}

	SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);
	return ctx;
}

struct simple_wait
{
	std::mutex mutex;
	std::condition_variable cond;
	int done = 0;

	void notify()
	{
		{
			std::lock_guard<std::mutex> lock(mutex);
			done = 1;
		}
		cond.notify_all();
	}

	bool wait(int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		return cond.wait_for(lock, std::chrono::milliseconds(timeout_ms),
							 [this]() { return done != 0; });
	}
};

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

	bool wait(int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		return cond.wait_for(lock, std::chrono::milliseconds(timeout_ms),
							 [this]() { return remaining == 0; });
	}
};

static void notify_fn(void *arg)
{
	static_cast<simple_wait *>(arg)->notify();
}

static void count_down_fn(void *arg)
{
	static_cast<count_wait *>(arg)->count_down();
}

static void add_one_cancel(void *arg, cancellation_type type)
{
	(void)type;
	++*static_cast<int *>(arg);
}

} /* namespace */

/* ---------- cancellation ---------- */

TEST(async_kernel, cancellation_emit_calls_handler_each_time)
{
	cancellation_signal signal;
	cancellation_slot slot = signal.slot();
	int calls = 0;

	EXPECT_TRUE(slot.is_connected());
	slot.assign(add_one_cancel, &calls);

	signal.emit(cancellation_type::all);
	EXPECT_EQ(1, calls);

	signal.emit(cancellation_type::terminal);
	EXPECT_EQ(2, calls);
}

TEST(async_kernel, cancellation_without_callback)
{
	cancellation_signal signal;

	signal.emit(cancellation_type::all);
	signal.emit(cancellation_type::terminal);
}

TEST(async_kernel, cancellation_none_does_not_emit)
{
	cancellation_signal signal;
	int calls = 0;

	signal.slot().assign(add_one_cancel, &calls);
	signal.emit(cancellation_type::none);

	EXPECT_EQ(0, calls);
}

TEST(async_kernel, cancellation_slot_clear_prevents_callback)
{
	cancellation_signal signal;
	cancellation_slot slot = signal.slot();
	int calls = 0;

	slot.assign(add_one_cancel, &calls);
	slot.clear();
	signal.emit(cancellation_type::all);

	EXPECT_EQ(0, calls);
}

/* ---------- io_context ---------- */

TEST(async_kernel, io_context_post_runs_on_run_thread)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	simple_wait wait;

	ASSERT_EQ(0, io.post(notify_fn, &wait));

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(wait.wait());
	io.stop();
	worker.join();
}

TEST(async_kernel, io_context_multiple_posts)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	simple_wait wait[4];

	for (int i = 0; i < 4; ++i)
		ASSERT_EQ(0, io.post(notify_fn, &wait[i]));

	std::thread worker([&io]() { io.run(); });
	for (int i = 0; i < 4; ++i)
		EXPECT_TRUE(wait[i].wait());
	io.stop();
	worker.join();
}

TEST(async_kernel, io_context_dispatch_is_inline_inside_worker)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	simple_wait wait;
	std::atomic<int> inner_done{0};
	std::atomic<int> outer_saw_inner{0};

	struct ctx_t
	{
		io_context *io;
		std::atomic<int> *inner_done;
		std::atomic<int> *outer_saw_inner;
		simple_wait *wait;
	} ctx{&io, &inner_done, &outer_saw_inner, &wait};

	ASSERT_EQ(0, io.post([](void *arg) {
		ctx_t *c = static_cast<ctx_t *>(arg);
		ASSERT_EQ(0, c->io->dispatch([](void *a) {
			++*static_cast<std::atomic<int> *>(a);
		}, c->inner_done));
		c->outer_saw_inner->store(c->inner_done->load());
		c->wait->notify();
	}, &ctx));

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(wait.wait());
	io.stop();
	worker.join();

	EXPECT_EQ(1, inner_done.load());
	EXPECT_EQ(1, outer_saw_inner.load());
}

TEST(async_kernel, io_context_defer_is_not_inline_inside_worker)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	simple_wait outer;
	simple_wait deferred;
	std::atomic<int> defer_done{0};
	std::atomic<int> saw_deferred{-1};

	struct defer_ctx_t
	{
		std::atomic<int> *defer_done;
		simple_wait *deferred;
	} defer_ctx{&defer_done, &deferred};

	struct ctx_t
	{
		io_context *io;
		std::atomic<int> *saw_deferred;
		simple_wait *outer;
		defer_ctx_t *defer_ctx;
	} ctx{&io, &saw_deferred, &outer, &defer_ctx};

	ASSERT_EQ(0, io.post([](void *arg) {
		ctx_t *c = static_cast<ctx_t *>(arg);
		ASSERT_EQ(0, c->io->defer([](void *a) {
			defer_ctx_t *d = static_cast<defer_ctx_t *>(a);
			++*d->defer_done;
			d->deferred->notify();
		}, c->defer_ctx));
		c->saw_deferred->store(c->defer_ctx->defer_done->load());
		c->outer->notify();
	}, &ctx));

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(outer.wait());
	EXPECT_EQ(0, saw_deferred.load());
	EXPECT_TRUE(deferred.wait());
	io.stop();
	worker.join();

	EXPECT_EQ(1, defer_done.load());
}

TEST(async_kernel, io_context_poll_processes_ready_ops)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	count_wait count;

	ASSERT_EQ(0, io.post(count_down_fn, &count));
	count.set(1);

	ASSERT_EQ(1, io.poll());
	EXPECT_EQ(0, count.remaining);
}

TEST(async_kernel, io_context_stop_restart)
{
	io_context io;
	ASSERT_EQ(0, io.init());

	io.stop();
	EXPECT_TRUE(io.stopped());

	io.restart();
	EXPECT_FALSE(io.stopped());
}

TEST(async_kernel, io_context_run_one_processes_single_op)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	simple_wait wait;

	ASSERT_EQ(0, io.post(notify_fn, &wait));
	ASSERT_EQ(1, io.run_one());
	EXPECT_TRUE(wait.wait());
}

static void set_errno_fn(void *)
{
	errno = EBADF;
}

TEST(async_kernel, io_context_handler_errno_does_not_stop_loop)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	simple_wait wait;

	ASSERT_EQ(0, io.post(set_errno_fn, nullptr));
	ASSERT_EQ(0, io.post(notify_fn, &wait));

	ASSERT_EQ(1, io.run_one());
	EXPECT_EQ(0, errno);

	ASSERT_EQ(1, io.run_one());
	EXPECT_TRUE(wait.wait());
}

TEST(async_kernel, io_context_poll_one_processes_single_op)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	simple_wait wait;

	ASSERT_EQ(0, io.post(notify_fn, &wait));
	ASSERT_EQ(1, io.poll_one());
	EXPECT_TRUE(wait.wait());
}

TEST(async_kernel, io_context_poll_one_no_work_returns)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	ASSERT_EQ(0, io.poll_one());
}

TEST(async_kernel, io_context_work_finished_stops_after_last_op)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	simple_wait wait;
	int iterations = 0;

	/* Keep the run loop alive until the posted op executes. */
	ASSERT_EQ(0, io.post(notify_fn, &wait));
	ASSERT_EQ(0, io.post([](void *arg) {
		++*static_cast<int *>(arg);
	}, &iterations));

	io.run();
	EXPECT_TRUE(wait.wait());
	EXPECT_EQ(1, iterations);
	EXPECT_TRUE(io.stopped());
}

/* ---------- strand ---------- */

TEST(async_kernel, explicit_factory_lifecycle)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	executor ex(io);

	strand *s = strand::create(&io);
	steady_timer *timer = steady_timer::create(ex);
	random_access_handle *file = random_access_handle::create(ex);
	tcp_socket *tcp = tcp_socket::create(ex);
	udp_socket *udp = udp_socket::create(ex);
	tcp_acceptor *acceptor = tcp_acceptor::create(ex);

	ASSERT_NE(nullptr, s);
	ASSERT_NE(nullptr, timer);
	ASSERT_NE(nullptr, file);
	ASSERT_NE(nullptr, tcp);
	ASSERT_NE(nullptr, udp);
	ASSERT_NE(nullptr, acceptor);

	tcp_acceptor::destroy(acceptor);
	udp_socket::destroy(udp);
	tcp_socket::destroy(tcp);
	random_access_handle::destroy(file);
	steady_timer::destroy(timer);
	strand::destroy(s);

	errno = 0;
	EXPECT_EQ(nullptr, ssl_stream::create(ex, nullptr, 0));
	EXPECT_EQ(EINVAL, errno);
}

TEST(async_kernel, strand_serial_execution)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	strand s;
	ASSERT_EQ(0, s.init(&io));
	std::mutex mutex;
	std::condition_variable cond;
	std::atomic<int> active{0};
	std::atomic<int> max_active{0};
	int completed = 0;
	const int total = 20;

	struct check_t
	{
		std::mutex *mutex;
		std::condition_variable *cond;
		std::atomic<int> *active;
		std::atomic<int> *max_active;
		int *completed;
		int total;
	} check{&mutex, &cond, &active, &max_active, &completed, total};

	for (int i = 0; i < total; ++i)
		ASSERT_EQ(0, s.post([](void *arg) {
			check_t *c = static_cast<check_t *>(arg);
			int a = ++*c->active;
			int cur = c->max_active->load();
			while (cur < a && !c->max_active->compare_exchange_weak(cur, a))
			{
			}
			std::this_thread::yield();
			--*c->active;
			std::lock_guard<std::mutex> lock(*c->mutex);
			++*c->completed;
			if (*c->completed == c->total)
				c->cond->notify_all();
		}, &check));

	std::thread worker([&io]() { io.run(); });
	{
		std::unique_lock<std::mutex> lock(mutex);
		EXPECT_TRUE(cond.wait_for(lock, std::chrono::seconds(5),
			[&]() { return completed == total; }));
	}

	EXPECT_EQ(1, max_active.load());
	EXPECT_EQ(total, completed);

	io.stop();
	worker.join();
}

TEST(async_kernel, strand_dispatch_inside_strand_is_inline)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	strand s;
	ASSERT_EQ(0, s.init(&io));
	simple_wait wait;
	std::atomic<int> inner_done{0};
	std::atomic<int> outer_done{0};

	struct ctx_t
	{
		strand *s;
		std::atomic<int> *inner_done;
		std::atomic<int> *outer_done;
		simple_wait *wait;
	} ctx{&s, &inner_done, &outer_done, &wait};

	ASSERT_EQ(0, s.post([](void *arg) {
		ctx_t *c = static_cast<ctx_t *>(arg);
		ASSERT_EQ(0, c->s->dispatch([](void *a) {
			++*static_cast<std::atomic<int> *>(a);
		}, c->inner_done));
		c->outer_done->store(c->inner_done->load());
		c->wait->notify();
	}, &ctx));

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(wait.wait());
	io.stop();
	worker.join();

	EXPECT_EQ(1, inner_done.load());
	EXPECT_EQ(1, outer_done.load());
}

TEST(async_kernel, strand_post_inside_strand_is_not_inline)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	strand s;
	ASSERT_EQ(0, s.init(&io));
	simple_wait outer;
	simple_wait inner;
	std::atomic<int> inner_done{0};
	std::atomic<int> outer_saw_inner{-1};

	struct inner_ctx_t
	{
		std::atomic<int> *inner_done;
		simple_wait *inner;
	} inner_ctx{&inner_done, &inner};

	struct ctx_t
	{
		strand *s;
		std::atomic<int> *outer_saw_inner;
		simple_wait *outer;
		inner_ctx_t *inner_ctx;
	} ctx{&s, &outer_saw_inner, &outer, &inner_ctx};

	ASSERT_EQ(0, s.post([](void *arg) {
		ctx_t *c = static_cast<ctx_t *>(arg);
		ASSERT_EQ(0, c->s->post([](void *a) {
			inner_ctx_t *d = static_cast<inner_ctx_t *>(a);
			++*d->inner_done;
			d->inner->notify();
		}, c->inner_ctx));
		c->outer_saw_inner->store(c->inner_ctx->inner_done->load());
		c->outer->notify();
	}, &ctx));

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(outer.wait());
	EXPECT_EQ(0, outer_saw_inner.load());
	EXPECT_TRUE(inner.wait());
	io.stop();
	worker.join();

	EXPECT_EQ(1, inner_done.load());
}

TEST(async_kernel, strand_concurrent_posts_are_serialized)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	strand s;
	ASSERT_EQ(0, s.init(&io));
	simple_wait done;
	std::atomic<int> active{0};
	std::atomic<int> max_active{0};
	std::atomic<int> executed{0};
	const int per_thread = 50;
	const int thread_count = 4;
	const int total = per_thread * thread_count;

	struct shared_t
	{
		std::atomic<int> *active;
		std::atomic<int> *max_active;
		std::atomic<int> *executed;
		simple_wait *done;
		int total;
	} shared{&active, &max_active, &executed, &done, total};

	std::vector<std::thread> posters;
	for (int t = 0; t < thread_count; ++t)
	{
		posters.emplace_back([&]() {
			for (int i = 0; i < per_thread; ++i)
			{
				ASSERT_EQ(0, s.post([](void *arg) {
					shared_t *c = static_cast<shared_t *>(arg);
					int a = ++*c->active;
					int cur = c->max_active->load();
					while (cur < a &&
						   !c->max_active->compare_exchange_weak(cur, a))
					{
					}
					std::this_thread::yield();
					--*c->active;
					if (++*c->executed == c->total)
						c->done->notify();
				}, &shared));
			}
		});
	}

	std::thread worker([&io]() { io.run(); });
	for (std::thread &t : posters)
		t.join();
	EXPECT_TRUE(done.wait(10000));
	guard.reset();
	io.stop();
	worker.join();

	EXPECT_EQ(total, executed.load());
	EXPECT_EQ(1, max_active.load());
}

TEST(async_kernel, strand_preserves_post_order)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	strand s;
	ASSERT_EQ(0, s.init(&io));
	std::vector<int> order;
	simple_wait done;
	const int total = 20;

	struct ctx_t
	{
		std::vector<int> *order;
		simple_wait *done;
		int total;
	} ctx{&order, &done, total};

	for (int i = 0; i < total; ++i)
	{
		ASSERT_EQ(0, s.post([](void *arg) {
			ctx_t *c = static_cast<ctx_t *>(arg);
			c->order->push_back((int)c->order->size());
			if ((int)c->order->size() == c->total)
				c->done->notify();
		}, &ctx));
	}

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(done.wait());
	guard.reset();
	io.stop();
	worker.join();

	ASSERT_EQ(total, (int)order.size());
	for (int i = 0; i < total; ++i)
		EXPECT_EQ(i, order[i]);
}

TEST(async_kernel, strand_copy_shares_state)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	strand s1;
	ASSERT_EQ(0, s1.init(&io));
	strand s2(s1);

	EXPECT_TRUE(s1 == s2);
	EXPECT_FALSE(s1 != s2);

	strand s3;

	ASSERT_EQ(0, s3.init(&io));
	s3 = s1;
	EXPECT_TRUE(s3 == s1);
}

TEST(async_kernel, strand_destroy_after_post_keeps_task_alive)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	simple_wait wait;

	{
		strand s;
		ASSERT_EQ(0, s.init(&io));
		ASSERT_EQ(0, s.post(notify_fn, &wait));
	}

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(wait.wait());
	io.stop();
	worker.join();
}

/* ---------- executor / executor_work_guard ---------- */

TEST(async_kernel, executor_io_context_post_runs)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	executor ex(io);
	simple_wait wait;

	ASSERT_EQ(0, ex.post(notify_fn, &wait));

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(wait.wait());
	io.stop();
	worker.join();
}

TEST(async_kernel, executor_strand_post_runs_and_serializes)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	strand s;
	ASSERT_EQ(0, s.init(&io));
	executor ex(s);
	simple_wait wait;

	ASSERT_EQ(0, ex.post(notify_fn, &wait));

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(wait.wait());
	io.stop();
	worker.join();
}

TEST(async_kernel, executor_keeps_strand_implementation_alive)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	executor ex;
	simple_wait wait;

	{
		strand s;
		ASSERT_EQ(0, s.init(&io));
		ex = executor(s);
	}

	ASSERT_EQ(0, ex.post(notify_fn, &wait));
	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(wait.wait());
	io.stop();
	worker.join();
}

TEST(async_kernel, strand_dispatch_survives_executor_destroyed_by_handler)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	executor *ex = nullptr;
	simple_wait wait;

	{
		strand s;
		ASSERT_EQ(0, s.init(&io));
		ex = new executor(s);
	}

	struct context
	{
		executor **ex;
		simple_wait *wait;
	} ctx{&ex, &wait};

	ASSERT_EQ(0, io.post(
		+[](void *arg) {
			context *ctx = static_cast<context *>(arg);
			ASSERT_EQ(0, (*ctx->ex)->dispatch(
				+[](void *inner) {
					context *ctx = static_cast<context *>(inner);
					delete *ctx->ex;
					*ctx->ex = nullptr;
					ctx->wait->notify();
				}, ctx));
		}, &ctx));

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(wait.wait());
	EXPECT_EQ(nullptr, ex);
	io.stop();
	worker.join();
}

TEST(async_kernel, executor_equality)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	strand s1;
	ASSERT_EQ(0, s1.init(&io));
	strand s2;
	ASSERT_EQ(0, s2.init(&io));
	executor io_ex1(io);
	executor io_ex2(io);
	executor s_ex1(s1);
	executor s_ex2(s1);
	executor s_ex3(s2);

	EXPECT_TRUE(io_ex1 == io_ex2);
	EXPECT_FALSE(io_ex1 != io_ex2);
	EXPECT_TRUE(s_ex1 == s_ex2);
	EXPECT_FALSE(s_ex1 == s_ex3);
	EXPECT_FALSE(io_ex1 == s_ex1);
}

TEST(async_kernel, executor_context_returns_io_context)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	executor ex(io);
	EXPECT_EQ(&io, ex.context());

	strand s;

	ASSERT_EQ(0, s.init(&io));
	executor sex(s);
	EXPECT_EQ(&io, sex.context());
}

TEST(async_kernel, executor_work_guard_keeps_run_alive_until_reset)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	simple_wait wait;

	ASSERT_TRUE(guard.owns_work());
	ASSERT_EQ(0, io.post(notify_fn, &wait));

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(wait.wait());
	EXPECT_FALSE(io.stopped());

	guard.reset();
	EXPECT_FALSE(guard.owns_work());
	worker.join();
}

/* ---------- steady_timer ---------- */

TEST(async_kernel, steady_timer_uses_associated_strand_executor)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	strand s;
	ASSERT_EQ(0, s.init(&io));
	steady_timer timer;
	ASSERT_EQ(0, timer.init(executor(s)));
	simple_wait wait;

	struct context
	{
		strand *serial;
		simple_wait *wait;
		std::atomic<int> in_strand;
	} ctx{&s, &wait, 0};

	timer.expires_after(std::chrono::milliseconds(1));
	ASSERT_EQ(0, timer.async_wait(
		+[](void *arg, async_error_code error) {
			context *ctx = static_cast<context *>(arg);
			if (!error && ctx->serial->running_in_this_thread())
				ctx->in_strand.store(1);
			ctx->wait->notify();
		}, &ctx));

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(wait.wait());
	EXPECT_EQ(1, ctx.in_strand.load());
	io.stop();
	worker.join();
}

struct timer_wait
{
	simple_wait wait;
	int error = -1;
};

static void timer_notify_fn(void *arg, async_error_code error)
{
	timer_wait *tw = static_cast<timer_wait *>(arg);
	tw->error = error_errno(error);
	tw->wait.notify();
}

TEST(async_kernel, steady_timer_fires)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	executor ex(io);
	steady_timer timer;
	ASSERT_EQ(0, timer.init(ex));
	timer_wait tw;

	timer.expires_after(std::chrono::milliseconds(10));
	ASSERT_EQ(0, timer.async_wait(timer_notify_fn, &tw));

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(tw.wait.wait());
	EXPECT_EQ(0, tw.error);

	guard.reset();
	io.stop();
	worker.join();
}

TEST(async_kernel, steady_timer_cancel_reports_ecanceled)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	executor ex(io);
	steady_timer timer;
	ASSERT_EQ(0, timer.init(ex));
	timer_wait tw;

	timer.expires_after(std::chrono::milliseconds(1000));
	ASSERT_EQ(0, timer.async_wait(timer_notify_fn, &tw));
	timer.cancel();

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(tw.wait.wait());
	EXPECT_EQ(ECANCELED, tw.error);

	guard.reset();
	io.stop();
	worker.join();
}

TEST(async_kernel, steady_timer_multiple_waits_all_fire)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	executor ex(io);
	steady_timer timer;
	ASSERT_EQ(0, timer.init(ex));
	timer_wait tw1;
	timer_wait tw2;

	timer.expires_after(std::chrono::milliseconds(10));
	ASSERT_EQ(0, timer.async_wait(timer_notify_fn, &tw1));
	ASSERT_EQ(0, timer.async_wait(timer_notify_fn, &tw2));

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(tw1.wait.wait());
	EXPECT_TRUE(tw2.wait.wait());
	EXPECT_EQ(0, tw1.error);
	EXPECT_EQ(0, tw2.error);

	guard.reset();
	io.stop();
	worker.join();
}

TEST(async_kernel, steady_timer_expires_after_cancels_pending_waits)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	executor ex(io);
	steady_timer timer;
	ASSERT_EQ(0, timer.init(ex));
	timer_wait tw1;
	timer_wait tw2;

	timer.expires_after(std::chrono::milliseconds(1000));
	ASSERT_EQ(0, timer.async_wait(timer_notify_fn, &tw1));

	/* Changing expiry cancels the pending wait and returns the count. */
	EXPECT_EQ(1u, timer.expires_after(std::chrono::milliseconds(10)));

	ASSERT_EQ(0, timer.async_wait(timer_notify_fn, &tw2));

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(tw1.wait.wait());
	EXPECT_TRUE(tw2.wait.wait());
	EXPECT_EQ(ECANCELED, tw1.error);
	EXPECT_EQ(0, tw2.error);

	guard.reset();
	io.stop();
	worker.join();
}

TEST(async_kernel, steady_timer_cancel_returns_count)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	executor ex(io);
	steady_timer timer;
	ASSERT_EQ(0, timer.init(ex));
	timer_wait tw1;
	timer_wait tw2;

	timer.expires_after(std::chrono::milliseconds(1000));
	ASSERT_EQ(0, timer.async_wait(timer_notify_fn, &tw1));
	ASSERT_EQ(0, timer.async_wait(timer_notify_fn, &tw2));

	EXPECT_EQ(2u, timer.cancel());

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(tw1.wait.wait());
	EXPECT_TRUE(tw2.wait.wait());
	EXPECT_EQ(ECANCELED, tw1.error);
	EXPECT_EQ(ECANCELED, tw2.error);

	guard.reset();
	io.stop();
	worker.join();
}

/* ---------- random_access_handle ---------- */

struct file_wait
{
	simple_wait wait;
	int error = -1;
	size_t bytes = 0;
	char buf[64];
};

static void file_read_cb(void *arg, async_error_code error, size_t bytes)
{
	file_wait *fw = static_cast<file_wait *>(arg);
	fw->error = error_errno(error);
	fw->bytes = bytes;
	fw->wait.notify();
}

static std::string make_temp_file_path()
{
	char dir[MAX_PATH];
	char path[MAX_PATH];

	GetTempPathA(MAX_PATH, dir);
	GetTempFileNameA(dir, "wfa", 0, path);
	return std::string(path);
}

TEST(async_kernel, random_access_handle_async_read)
{
	std::string path = make_temp_file_path();
	HANDLE h = CreateFileA(path.c_str(),
		GENERIC_READ | GENERIC_WRITE,
		FILE_SHARE_READ | FILE_SHARE_WRITE,
		nullptr,
		CREATE_ALWAYS,
		0,
		nullptr);
	ASSERT_TRUE(h != INVALID_HANDLE_VALUE);

	static const char data[] = "hello";
	DWORD written = 0;
	ASSERT_TRUE(WriteFile(h, data, 5, &written, nullptr));
	ASSERT_EQ(5u, written);
	CloseHandle(h);

	h = CreateFileA(path.c_str(),
		GENERIC_READ | GENERIC_WRITE,
		FILE_SHARE_READ | FILE_SHARE_WRITE,
		nullptr,
		OPEN_EXISTING,
		FILE_FLAG_OVERLAPPED,
		nullptr);
	ASSERT_TRUE(h != INVALID_HANDLE_VALUE);

	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	random_access_handle fh;
	ASSERT_EQ(0, fh.init(executor(io)));
	ASSERT_EQ(0, fh.assign(h));

	file_wait fw;
	memset(fw.buf, 0, sizeof fw.buf);
	ASSERT_EQ(0, fh.async_read_some_at(0, fw.buf, 5, file_read_cb, &fw));

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(fw.wait.wait());
	EXPECT_EQ(0, fw.error);
	EXPECT_EQ(5u, fw.bytes);
	EXPECT_EQ(0, memcmp(fw.buf, data, 5));

	fh.close();
	guard.reset();
	io.stop();
	worker.join();
	DeleteFileA(path.c_str());
}

TEST(async_kernel, random_access_handle_async_write)
{
	std::string path = make_temp_file_path();
	HANDLE h = CreateFileA(path.c_str(),
		GENERIC_READ | GENERIC_WRITE,
		FILE_SHARE_READ | FILE_SHARE_WRITE,
		nullptr,
		CREATE_ALWAYS,
		FILE_FLAG_OVERLAPPED,
		nullptr);
	ASSERT_TRUE(h != INVALID_HANDLE_VALUE);

	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	random_access_handle fh;
	ASSERT_EQ(0, fh.init(executor(io)));
	ASSERT_EQ(0, fh.assign(h));

	static const char data[] = "world";
	file_wait fw;
	ASSERT_EQ(0, fh.async_write_some_at(0, data, 5, file_read_cb, &fw));

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(fw.wait.wait());
	EXPECT_EQ(0, fw.error);
	EXPECT_EQ(5u, fw.bytes);

	fh.close();
	guard.reset();
	io.stop();
	worker.join();

	HANDLE rh = CreateFileA(path.c_str(),
		GENERIC_READ,
		FILE_SHARE_READ,
		nullptr,
		OPEN_EXISTING,
		0,
		nullptr);
	ASSERT_TRUE(rh != INVALID_HANDLE_VALUE);
	char buf[16] = {};
	DWORD readed = 0;
	ASSERT_TRUE(ReadFile(rh, buf, 5, &readed, nullptr));
	ASSERT_EQ(5u, readed);
	EXPECT_EQ(0, memcmp(buf, data, 5));
	CloseHandle(rh);
	DeleteFileA(path.c_str());
}

TEST(async_kernel, random_access_handle_release_keeps_handle_open)
{
	std::string path = make_temp_file_path();
	HANDLE h = CreateFileA(path.c_str(),
		GENERIC_READ | GENERIC_WRITE,
		FILE_SHARE_READ | FILE_SHARE_WRITE,
		nullptr,
		CREATE_ALWAYS,
		FILE_FLAG_OVERLAPPED,
		nullptr);
	ASSERT_TRUE(h != INVALID_HANDLE_VALUE);

	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	random_access_handle fh;
	ASSERT_EQ(0, fh.init(executor(io)));
	ASSERT_EQ(0, fh.assign(h));
	ASSERT_EQ(0, fh.release());
	ASSERT_EQ(INVALID_HANDLE_VALUE, fh.native_handle());

	// The user-owned HANDLE must still be open after release.
	ASSERT_NE(GetFileSize(h, nullptr), INVALID_FILE_SIZE);

	fh.close();
	guard.reset();
	io.stop();
	CloseHandle(h);
	DeleteFileA(path.c_str());
}

TEST(async_kernel, random_access_handle_async_readv_writev)
{
	std::string path = make_temp_file_path();
	HANDLE h = CreateFileA(path.c_str(),
		GENERIC_READ | GENERIC_WRITE,
		FILE_SHARE_READ | FILE_SHARE_WRITE,
		nullptr,
		CREATE_ALWAYS,
		FILE_FLAG_OVERLAPPED,
		nullptr);
	ASSERT_TRUE(h != INVALID_HANDLE_VALUE);

	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	random_access_handle fh;
	ASSERT_EQ(0, fh.init(executor(io)));
	ASSERT_EQ(0, fh.assign(h));

	char wbuf[8] = "hello!!";
	struct iovec wiov[2] = {{wbuf, 2}, {wbuf + 2, 3}};
	file_wait ww;
	ASSERT_EQ(0, fh.async_writev_at(0, wiov, 2, file_read_cb, &ww));

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(ww.wait.wait());
	EXPECT_EQ(0, ww.error);
	EXPECT_EQ(5u, ww.bytes);

	file_wait rr;
	memset(rr.buf, 0, sizeof rr.buf);
	struct iovec riov[2] = {{rr.buf, 2}, {rr.buf + 2, 3}};
	ASSERT_EQ(0, fh.async_readv_at(0, riov, 2, file_read_cb, &rr));
	EXPECT_TRUE(rr.wait.wait());
	EXPECT_EQ(0, rr.error);
	EXPECT_EQ(5u, rr.bytes);
	EXPECT_EQ(0, memcmp(rr.buf, "hello", 5));

	fh.close();
	guard.reset();
	io.stop();
	worker.join();
	DeleteFileA(path.c_str());
}

/* ---------- tcp_socket ---------- */

struct connect_wait
{
	simple_wait wait;
	int error = -1;
};

static void connect_cb(void *arg, async_error_code error)
{
	connect_wait *cw = static_cast<connect_wait *>(arg);
	cw->error = error_errno(error);
	cw->wait.notify();
}

struct rw_wait
{
	simple_wait wait;
	int error = -1;
	size_t bytes = 0;
	char buf[32];
};

static void rw_cb(void *arg, async_error_code error, size_t bytes)
{
	rw_wait *rw = static_cast<rw_wait *>(arg);
	rw->error = error_errno(error);
	rw->bytes = bytes;
	rw->wait.notify();
}

TEST(async_kernel, tcp_socket_connect_write_read)
{
	SOCKET listener = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(listener != INVALID_SOCKET);

	struct sockaddr_in addr;
	memset(&addr, 0, sizeof addr);
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = 0;

	ASSERT_EQ(0, ::bind(listener, (struct sockaddr *)&addr, sizeof addr));
	ASSERT_EQ(0, ::listen(listener, 1));
	int addrlen = sizeof addr;
	ASSERT_EQ(0, ::getsockname(listener, (struct sockaddr *)&addr, &addrlen));

	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	tcp_socket client;
	ASSERT_EQ(0, client.init(executor(io)));
	ASSERT_EQ(0, client.open());

	connect_wait cw;
	ASSERT_EQ(0, client.async_connect((struct sockaddr *)&addr,
									  sizeof addr, connect_cb, &cw));

	std::thread server([&]() {
		SOCKET c = ::accept(listener, nullptr, nullptr);
		if (c == INVALID_SOCKET)
			return;
		::send(c, "pong", 4, 0);
		char buf[16];
		::recv(c, buf, 4, 0);
		::closesocket(c);
	});

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(cw.wait.wait());
	EXPECT_EQ(0, cw.error);

	rw_wait ww;
	ASSERT_EQ(0, client.async_write_some("ping", 4, rw_cb, &ww));
	EXPECT_TRUE(ww.wait.wait());
	EXPECT_EQ(0, ww.error);
	EXPECT_EQ(4u, ww.bytes);

	rw_wait rr;
	ASSERT_EQ(0, client.async_read_some(rr.buf, 4, rw_cb, &rr));
	EXPECT_TRUE(rr.wait.wait());
	EXPECT_EQ(0, rr.error);
	EXPECT_EQ(4u, rr.bytes);
	EXPECT_EQ(0, memcmp(rr.buf, "pong", 4));

	client.close();
	guard.reset();
	io.stop();
	worker.join();
	server.join();
	::closesocket(listener);
}

TEST(async_kernel, tcp_socket_read_and_write_can_overlap)
{
	SOCKET listener = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(listener != INVALID_SOCKET);

	struct sockaddr_in addr;
	memset(&addr, 0, sizeof addr);
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = 0;
	ASSERT_EQ(0, ::bind(listener, (struct sockaddr *)&addr, sizeof addr));
	ASSERT_EQ(0, ::listen(listener, 1));
	int addrlen = sizeof addr;
	ASSERT_EQ(0, ::getsockname(listener, (struct sockaddr *)&addr, &addrlen));

	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	tcp_socket client;
	ASSERT_EQ(0, client.init(executor(io)));
	ASSERT_EQ(0, client.open());
	connect_wait cw;
	ASSERT_EQ(0, client.async_connect((struct sockaddr *)&addr,
									  sizeof addr, connect_cb, &cw));

	std::thread server([&]() {
		SOCKET c = ::accept(listener, nullptr, nullptr);
		if (c == INVALID_SOCKET)
			return;
		char buf[4];
		if (::recv(c, buf, sizeof buf, 0) == sizeof buf)
			::send(c, "pong", 4, 0);
		::closesocket(c);
	});
	std::thread worker([&io]() { io.run(); });
	ASSERT_TRUE(cw.wait.wait());
	ASSERT_EQ(0, cw.error);

	rw_wait rr;
	rw_wait ww;
	ASSERT_EQ(0, client.async_read_some(rr.buf, 4, rw_cb, &rr));
	ASSERT_EQ(0, client.async_write_some("ping", 4, rw_cb, &ww));
	EXPECT_TRUE(ww.wait.wait());
	EXPECT_TRUE(rr.wait.wait());
	EXPECT_EQ(0, ww.error);
	EXPECT_EQ(4u, ww.bytes);
	EXPECT_EQ(0, rr.error);
	EXPECT_EQ(4u, rr.bytes);
	EXPECT_EQ(0, memcmp(rr.buf, "pong", 4));

	client.close();
	guard.reset();
	io.stop();
	worker.join();
	server.join();
	::closesocket(listener);
}

TEST(async_kernel, tcp_socket_wait_read_does_not_consume_data)
{
	SOCKET listener = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(listener != INVALID_SOCKET);

	struct sockaddr_in addr;
	memset(&addr, 0, sizeof addr);
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = 0;
	ASSERT_EQ(0, ::bind(listener, (struct sockaddr *)&addr, sizeof addr));
	ASSERT_EQ(0, ::listen(listener, 1));
	int addrlen = sizeof addr;
	ASSERT_EQ(0, ::getsockname(listener, (struct sockaddr *)&addr, &addrlen));

	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	tcp_socket client;
	ASSERT_EQ(0, client.init(executor(io)));
	ASSERT_EQ(0, client.open());
	connect_wait cw;
	ASSERT_EQ(0, client.async_connect((struct sockaddr *)&addr,
									  sizeof addr, connect_cb, &cw));

	std::thread server([&]() {
		SOCKET c = ::accept(listener, nullptr, nullptr);
		if (c == INVALID_SOCKET)
			return;
		::send(c, "data", 4, 0);
		::Sleep(100);
		::closesocket(c);
	});
	std::thread worker([&io]() { io.run(); });
	ASSERT_TRUE(cw.wait.wait());
	ASSERT_EQ(0, cw.error);

	rw_wait ready;
	ASSERT_EQ(0, client.async_wait_read(rw_cb, &ready));
	ASSERT_TRUE(ready.wait.wait());
	EXPECT_EQ(0, ready.error);
	EXPECT_EQ(0u, ready.bytes);
	char buf[4];
	EXPECT_EQ(4, ::recv(client.native_handle(), buf, sizeof buf, 0));
	EXPECT_EQ(0, memcmp(buf, "data", 4));

	client.close();
	guard.reset();
	io.stop();
	worker.join();
	server.join();
	::closesocket(listener);
}

TEST(async_kernel, tcp_socket_writev_readv)
{
	SOCKET listener = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(listener != INVALID_SOCKET);

	struct sockaddr_in addr;
	memset(&addr, 0, sizeof addr);
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = 0;

	ASSERT_EQ(0, ::bind(listener, (struct sockaddr *)&addr, sizeof addr));
	ASSERT_EQ(0, ::listen(listener, 1));
	int addrlen = sizeof addr;
	ASSERT_EQ(0, ::getsockname(listener, (struct sockaddr *)&addr, &addrlen));

	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	tcp_socket client;
	ASSERT_EQ(0, client.init(executor(io)));
	ASSERT_EQ(0, client.open());

	connect_wait cw;
	ASSERT_EQ(0, client.async_connect((struct sockaddr *)&addr,
									  sizeof addr, connect_cb, &cw));

	std::thread server([&]() {
		SOCKET c = ::accept(listener, nullptr, nullptr);
		if (c == INVALID_SOCKET)
			return;
		char buf[16];
		::recv(c, buf, 4, 0);
		::send(c, "pong", 4, 0);
		::closesocket(c);
	});

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(cw.wait.wait());
	EXPECT_EQ(0, cw.error);

	char wbuf[8] = "hello!!";
	struct iovec wiov[2] = {{wbuf, 2}, {wbuf + 2, 2}};
	rw_wait ww;
	ASSERT_EQ(0, client.async_writev_some(wiov, 2, rw_cb, &ww));
	EXPECT_TRUE(ww.wait.wait());
	EXPECT_EQ(0, ww.error);
	EXPECT_EQ(4u, ww.bytes);

	rw_wait rr;
	struct iovec riov[2] = {{rr.buf, 2}, {rr.buf + 2, 2}};
	ASSERT_EQ(0, client.async_readv_some(riov, 2, rw_cb, &rr));
	EXPECT_TRUE(rr.wait.wait());
	EXPECT_EQ(0, rr.error);
	EXPECT_EQ(4u, rr.bytes);
	EXPECT_EQ(0, memcmp(rr.buf, "pong", 4));

	client.close();
	guard.reset();
	io.stop();
	worker.join();
	server.join();
	::closesocket(listener);
}

TEST(async_kernel, tcp_socket_cancel_reports_ecanceled)
{
	SOCKET listener = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(listener != INVALID_SOCKET);

	struct sockaddr_in addr;
	memset(&addr, 0, sizeof addr);
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = 0;

	ASSERT_EQ(0, ::bind(listener, (struct sockaddr *)&addr, sizeof addr));
	ASSERT_EQ(0, ::listen(listener, 1));
	int addrlen = sizeof addr;
	ASSERT_EQ(0, ::getsockname(listener, (struct sockaddr *)&addr, &addrlen));

	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	tcp_socket client;
	ASSERT_EQ(0, client.init(executor(io)));
	ASSERT_EQ(0, client.open());

	connect_wait cw;
	ASSERT_EQ(0, client.async_connect((struct sockaddr *)&addr,
									  sizeof addr, connect_cb, &cw));

	std::thread server([&]() {
		SOCKET c = ::accept(listener, nullptr, nullptr);
		if (c == INVALID_SOCKET)
			return;
		::Sleep(1000);
		::closesocket(c);
	});

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(cw.wait.wait());
	EXPECT_EQ(0, cw.error);

	rw_wait rr;
	ASSERT_EQ(0, client.async_read_some(rr.buf, 4, rw_cb, &rr));
	ASSERT_EQ(0, client.cancel());
	EXPECT_TRUE(rr.wait.wait());
	EXPECT_EQ(ECANCELED, rr.error);

	client.close();
	guard.reset();
	io.stop();
	worker.join();
	server.join();
	::closesocket(listener);
}

static int msg_filter_5(void *buffer, size_t *size, void *userdata)
{
	(void)buffer;
	(void)userdata;
	if (*size >= 5)
		return 1;
	*size = 0;
	return 0;
}

TEST(async_kernel, read_message_accumulates_until_filter_accepts)
{
	SOCKET listener = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(listener != INVALID_SOCKET);

	struct sockaddr_in addr;
	memset(&addr, 0, sizeof addr);
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = 0;

	ASSERT_EQ(0, ::bind(listener, (struct sockaddr *)&addr, sizeof addr));
	ASSERT_EQ(0, ::listen(listener, 1));
	int addrlen = sizeof addr;
	ASSERT_EQ(0, ::getsockname(listener, (struct sockaddr *)&addr, &addrlen));

	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	tcp_socket reader;
	ASSERT_EQ(0, reader.init(executor(io)));
	ASSERT_EQ(0, reader.open());

	connect_wait cw;
	ASSERT_EQ(0, reader.async_connect((struct sockaddr *)&addr,
									  sizeof addr, connect_cb, &cw));

	std::thread server([&]() {
		SOCKET c = ::accept(listener, nullptr, nullptr);
		if (c == INVALID_SOCKET)
			return;
		::send(c, "he", 2, 0);
		::Sleep(50);
		::send(c, "llo", 3, 0);
		::closesocket(c);
	});

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(cw.wait.wait());
	EXPECT_EQ(0, cw.error);

	rw_wait rr;
	ASSERT_EQ(0, async_read_message(&reader, rr.buf, sizeof rr.buf,
									msg_filter_5, nullptr, rw_cb, &rr));
	EXPECT_TRUE(rr.wait.wait());
	EXPECT_EQ(0, rr.error);
	EXPECT_EQ(5u, rr.bytes);
	EXPECT_EQ(0, memcmp(rr.buf, "hello", 5));

	reader.close();
	guard.reset();
	io.stop();
	worker.join();
	server.join();
	::closesocket(listener);
}

TEST(async_kernel, read_message_timeout_reports_etimedout)
{
	SOCKET listener = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(listener != INVALID_SOCKET);

	struct sockaddr_in addr;
	memset(&addr, 0, sizeof addr);
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = 0;

	ASSERT_EQ(0, ::bind(listener, (struct sockaddr *)&addr, sizeof addr));
	ASSERT_EQ(0, ::listen(listener, 1));
	int addrlen = sizeof addr;
	ASSERT_EQ(0, ::getsockname(listener, (struct sockaddr *)&addr, &addrlen));

	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	tcp_socket reader;
	ASSERT_EQ(0, reader.init(executor(io)));
	ASSERT_EQ(0, reader.open());

	connect_wait cw;
	ASSERT_EQ(0, reader.async_connect((struct sockaddr *)&addr,
									  sizeof addr, connect_cb, &cw));

	std::thread server([&]() {
		SOCKET c = ::accept(listener, nullptr, nullptr);
		if (c == INVALID_SOCKET)
			return;
		::Sleep(1000);
		::closesocket(c);
	});

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(cw.wait.wait());
	EXPECT_EQ(0, cw.error);

	rw_wait rr;
	ASSERT_EQ(0, async_read_message(&reader, rr.buf, sizeof rr.buf,
									msg_filter_5, nullptr, rw_cb, &rr, 50));
	EXPECT_TRUE(rr.wait.wait());
	EXPECT_EQ(ETIMEDOUT, rr.error);

	reader.close();
	guard.reset();
	io.stop();
	worker.join();
	server.join();
	::closesocket(listener);
}

/* ---------- udp_socket ---------- */

TEST(async_kernel, udp_socket_send_receive)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	udp_socket receiver;
	ASSERT_EQ(0, receiver.init(executor(io)));
	udp_socket sender;
	ASSERT_EQ(0, sender.init(executor(io)));

	ASSERT_EQ(0, receiver.open());
	ASSERT_EQ(0, sender.open());

	struct sockaddr_in bind_addr;
	memset(&bind_addr, 0, sizeof bind_addr);
	bind_addr.sin_family = AF_INET;
	bind_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	bind_addr.sin_port = 0;

	ASSERT_EQ(0, receiver.bind((struct sockaddr *)&bind_addr,
							   sizeof bind_addr));
	int addrlen = sizeof bind_addr;
	ASSERT_EQ(0, ::getsockname(receiver.native_handle(),
							   (struct sockaddr *)&bind_addr, &addrlen));

	rw_wait rr;
	struct sockaddr_in from;
	int fromlen = sizeof from;
	memset(&from, 0, sizeof from);
	ASSERT_EQ(0, receiver.async_receive_from(rr.buf, 4,
		(struct sockaddr *)&from, &fromlen, rw_cb, &rr));

	std::thread worker([&io]() { io.run(); });

	rw_wait sw;
	ASSERT_EQ(0, sender.async_send_to("ping", 4,
		(struct sockaddr *)&bind_addr, sizeof bind_addr, rw_cb, &sw));

	EXPECT_TRUE(sw.wait.wait());
	EXPECT_EQ(0, sw.error);
	EXPECT_EQ(4u, sw.bytes);

	EXPECT_TRUE(rr.wait.wait());
	EXPECT_EQ(0, rr.error);
	EXPECT_EQ(4u, rr.bytes);
	EXPECT_EQ(0, memcmp(rr.buf, "ping", 4));
	EXPECT_GT(fromlen, 0);

	receiver.close();
	sender.close();
	guard.reset();
	io.stop();
	worker.join();
}

static int udp_msg_filter(void *buffer, size_t *size, void *userdata)
{
	(void)buffer;
	(void)userdata;
	if (*size >= 4)
	{
		*size = 4;
		return 1;
	}
	*size = 0;
	return 0;
}

TEST(async_kernel, udp_recvfrom_message_applies_filter)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	udp_socket receiver;
	ASSERT_EQ(0, receiver.init(executor(io)));
	udp_socket sender;
	ASSERT_EQ(0, sender.init(executor(io)));

	ASSERT_EQ(0, receiver.open());
	ASSERT_EQ(0, sender.open());

	struct sockaddr_in bind_addr;
	memset(&bind_addr, 0, sizeof bind_addr);
	bind_addr.sin_family = AF_INET;
	bind_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	bind_addr.sin_port = 0;

	ASSERT_EQ(0, receiver.bind((struct sockaddr *)&bind_addr,
							   sizeof bind_addr));
	int addrlen = sizeof bind_addr;
	ASSERT_EQ(0, ::getsockname(receiver.native_handle(),
							   (struct sockaddr *)&bind_addr, &addrlen));

	rw_wait rr;
	struct sockaddr_in from;
	int fromlen = sizeof from;
	memset(&from, 0, sizeof from);
	ASSERT_EQ(0, async_recvfrom_message(&receiver, rr.buf, sizeof rr.buf,
		(struct sockaddr *)&from, &fromlen, udp_msg_filter, nullptr,
		rw_cb, &rr));

	std::thread worker([&io]() { io.run(); });

	rw_wait sw;
	ASSERT_EQ(0, sender.async_send_to("ping", 4,
		(struct sockaddr *)&bind_addr, sizeof bind_addr, rw_cb, &sw));

	EXPECT_TRUE(sw.wait.wait());
	EXPECT_EQ(0, sw.error);
	EXPECT_TRUE(rr.wait.wait());
	EXPECT_EQ(0, rr.error);
	EXPECT_EQ(4u, rr.bytes);
	EXPECT_EQ(0, memcmp(rr.buf, "ping", 4));
	EXPECT_GT(fromlen, 0);

	receiver.close();
	sender.close();
	guard.reset();
	io.stop();
	worker.join();
}

struct udp_retry_filter_state
{
	int calls = 0;
};

static int udp_msg_filter_retry_once(void *buffer, size_t *size,
									 void *userdata)
{
	udp_retry_filter_state *st = static_cast<udp_retry_filter_state *>(userdata);
	++st->calls;
	if (st->calls == 1)
	{
		*size = 0;
		return 0;
	}
	(void)buffer;
	if (*size >= 4)
	{
		*size = 4;
		return 1;
	}
	*size = 0;
	return 0;
}

TEST(async_kernel, udp_recvfrom_message_retries_when_filter_returns_zero)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	udp_socket receiver;
	ASSERT_EQ(0, receiver.init(executor(io)));
	udp_socket sender;
	ASSERT_EQ(0, sender.init(executor(io)));

	ASSERT_EQ(0, receiver.open());
	ASSERT_EQ(0, sender.open());

	struct sockaddr_in bind_addr;
	memset(&bind_addr, 0, sizeof bind_addr);
	bind_addr.sin_family = AF_INET;
	bind_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	bind_addr.sin_port = 0;

	ASSERT_EQ(0, receiver.bind((struct sockaddr *)&bind_addr,
							   sizeof bind_addr));
	int addrlen = sizeof bind_addr;
	ASSERT_EQ(0, ::getsockname(receiver.native_handle(),
							   (struct sockaddr *)&bind_addr, &addrlen));

	rw_wait rr;
	struct sockaddr_in from;
	int fromlen = sizeof from;
	memset(&from, 0, sizeof from);
	udp_retry_filter_state state;
	ASSERT_EQ(0, async_recvfrom_message(&receiver, rr.buf, sizeof rr.buf,
		(struct sockaddr *)&from, &fromlen, udp_msg_filter_retry_once, &state,
		rw_cb, &rr));

	std::thread worker([&io]() { io.run(); });

	rw_wait sw;
	ASSERT_EQ(0, sender.async_send_to("ping", 4,
		(struct sockaddr *)&bind_addr, sizeof bind_addr, rw_cb, &sw));
	EXPECT_TRUE(sw.wait.wait());
	EXPECT_EQ(0, sw.error);

	rw_wait sw2;
	ASSERT_EQ(0, sender.async_send_to("pong", 4,
		(struct sockaddr *)&bind_addr, sizeof bind_addr, rw_cb, &sw2));
	EXPECT_TRUE(sw2.wait.wait());
	EXPECT_EQ(0, sw2.error);

	EXPECT_TRUE(rr.wait.wait());
	EXPECT_EQ(0, rr.error);
	EXPECT_EQ(4u, rr.bytes);
	EXPECT_EQ(0, memcmp(rr.buf, "pong", 4));
	EXPECT_EQ(2, state.calls);

	receiver.close();
	sender.close();
	guard.reset();
	io.stop();
	worker.join();
}

TEST(async_kernel, udp_recvfrom_message_timeout_reports_etimedout)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	udp_socket receiver;
	ASSERT_EQ(0, receiver.init(executor(io)));

	ASSERT_EQ(0, receiver.open());

	struct sockaddr_in bind_addr;
	memset(&bind_addr, 0, sizeof bind_addr);
	bind_addr.sin_family = AF_INET;
	bind_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	bind_addr.sin_port = 0;

	ASSERT_EQ(0, receiver.bind((struct sockaddr *)&bind_addr,
							   sizeof bind_addr));

	rw_wait rr;
	struct sockaddr_in from;
	int fromlen = sizeof from;
	memset(&from, 0, sizeof from);
	ASSERT_EQ(0, async_recvfrom_message(&receiver, rr.buf, sizeof rr.buf,
		(struct sockaddr *)&from, &fromlen, udp_msg_filter, nullptr,
		rw_cb, &rr, 50));

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(rr.wait.wait());
	EXPECT_EQ(ETIMEDOUT, rr.error);

	receiver.close();
	guard.reset();
	io.stop();
	worker.join();
}

TEST(async_kernel, udp_socket_sendto_v_receive)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	udp_socket receiver;
	ASSERT_EQ(0, receiver.init(executor(io)));
	udp_socket sender;
	ASSERT_EQ(0, sender.init(executor(io)));

	ASSERT_EQ(0, receiver.open());
	ASSERT_EQ(0, sender.open());

	struct sockaddr_in bind_addr;
	memset(&bind_addr, 0, sizeof bind_addr);
	bind_addr.sin_family = AF_INET;
	bind_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	bind_addr.sin_port = 0;

	ASSERT_EQ(0, receiver.bind((struct sockaddr *)&bind_addr,
							   sizeof bind_addr));
	int addrlen = sizeof bind_addr;
	ASSERT_EQ(0, ::getsockname(receiver.native_handle(),
							   (struct sockaddr *)&bind_addr, &addrlen));

	rw_wait rr;
	struct sockaddr_in from;
	int fromlen = sizeof from;
	memset(&from, 0, sizeof from);
	ASSERT_EQ(0, receiver.async_receive_from(rr.buf, 4,
		(struct sockaddr *)&from, &fromlen, rw_cb, &rr));

	std::thread worker([&io]() { io.run(); });

	char data[] = "ping";
	struct iovec iov[2] = {{data, 2}, {data + 2, 2}};
	rw_wait sw;
	ASSERT_EQ(0, sender.async_sendto_v(iov, 2,
		(struct sockaddr *)&bind_addr, sizeof bind_addr, rw_cb, &sw));

	EXPECT_TRUE(sw.wait.wait());
	EXPECT_EQ(0, sw.error);
	EXPECT_EQ(4u, sw.bytes);

	EXPECT_TRUE(rr.wait.wait());
	EXPECT_EQ(0, rr.error);
	EXPECT_EQ(4u, rr.bytes);
	EXPECT_EQ(0, memcmp(rr.buf, "ping", 4));
	EXPECT_GT(fromlen, 0);

	receiver.close();
	sender.close();
	guard.reset();
	io.stop();
	worker.join();
}

TEST(async_kernel, udp_socket_read_and_write_can_overlap)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	udp_socket first;
	ASSERT_EQ(0, first.init(executor(io)));
	udp_socket second;
	ASSERT_EQ(0, second.init(executor(io)));
	ASSERT_EQ(0, first.open());
	ASSERT_EQ(0, second.open());

	struct sockaddr_in first_addr;
	struct sockaddr_in second_addr;
	memset(&first_addr, 0, sizeof first_addr);
	memset(&second_addr, 0, sizeof second_addr);
	first_addr.sin_family = AF_INET;
	second_addr.sin_family = AF_INET;
	first_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	second_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	ASSERT_EQ(0, ::bind(first.native_handle(),
		(struct sockaddr *)&first_addr, sizeof first_addr));
	ASSERT_EQ(0, ::bind(second.native_handle(),
		(struct sockaddr *)&second_addr, sizeof second_addr));
	int addrlen = sizeof first_addr;
	ASSERT_EQ(0, ::getsockname(first.native_handle(),
		(struct sockaddr *)&first_addr, &addrlen));
	addrlen = sizeof second_addr;
	ASSERT_EQ(0, ::getsockname(second.native_handle(),
		(struct sockaddr *)&second_addr, &addrlen));

	rw_wait first_read;
	struct sockaddr_in first_from;
	int first_fromlen = sizeof first_from;
	ASSERT_EQ(0, first.async_receive_from(first_read.buf,
		sizeof first_read.buf, (struct sockaddr *)&first_from,
		&first_fromlen, rw_cb, &first_read));

	rw_wait second_read;
	struct sockaddr_in second_from;
	int second_fromlen = sizeof second_from;
	ASSERT_EQ(0, second.async_receive_from(second_read.buf,
		sizeof second_read.buf, (struct sockaddr *)&second_from,
		&second_fromlen, rw_cb, &second_read));

	rw_wait first_write;
	ASSERT_EQ(0, first.async_send_to("ping", 4,
		(struct sockaddr *)&second_addr, sizeof second_addr,
		rw_cb, &first_write));

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(first_write.wait.wait());
	EXPECT_EQ(0, first_write.error);
	EXPECT_TRUE(second_read.wait.wait());
	EXPECT_EQ(0, second_read.error);
	EXPECT_EQ(0, memcmp(second_read.buf, "ping", 4));

	ASSERT_EQ(0, first.cancel_read());
	EXPECT_TRUE(first_read.wait.wait());
	EXPECT_EQ(ECANCELED, first_read.error);
	first.close();
	second.close();
	guard.reset();
	io.stop();
	worker.join();
}

TEST(async_kernel, udp_socket_cancel_reports_ecanceled)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	udp_socket receiver;
	ASSERT_EQ(0, receiver.init(executor(io)));
	ASSERT_EQ(0, receiver.open());

	struct sockaddr_in bind_addr;
	memset(&bind_addr, 0, sizeof bind_addr);
	bind_addr.sin_family = AF_INET;
	bind_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	bind_addr.sin_port = 0;
	ASSERT_EQ(0, receiver.bind((struct sockaddr *)&bind_addr,
							   sizeof bind_addr));

	rw_wait rr;
	struct sockaddr_in from;
	int fromlen = sizeof from;
	ASSERT_EQ(0, receiver.async_receive_from(rr.buf, 4,
		(struct sockaddr *)&from, &fromlen, rw_cb, &rr));
	ASSERT_EQ(0, receiver.cancel());

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(rr.wait.wait());
	EXPECT_EQ(ECANCELED, rr.error);

	receiver.close();
	guard.reset();
	io.stop();
	worker.join();
}

/* ---------- tcp_acceptor ---------- */

struct accept_wait
{
	simple_wait wait;
	int error = -1;
	SOCKET socket = INVALID_SOCKET;
};

static void accept_cb(void *arg, async_error_code error, SOCKET socket)
{
	accept_wait *aw = static_cast<accept_wait *>(arg);
	aw->error = error_errno(error);
	aw->socket = socket;
	aw->wait.notify();
}

TEST(async_kernel, tcp_acceptor_async_accept)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	tcp_acceptor acceptor;
	ASSERT_EQ(0, acceptor.init(executor(io)));

	ASSERT_EQ(0, acceptor.open());

	struct sockaddr_in addr;
	memset(&addr, 0, sizeof addr);
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = 0;

	ASSERT_EQ(0, acceptor.bind((struct sockaddr *)&addr, sizeof addr));
	ASSERT_EQ(0, acceptor.listen(16));
	int addrlen = sizeof addr;
	ASSERT_EQ(0, ::getsockname(acceptor.native_handle(),
							   (struct sockaddr *)&addr, &addrlen));

	accept_wait aw;
	ASSERT_EQ(0, acceptor.async_accept(accept_cb, &aw));

	std::thread worker([&io]() { io.run(); });

	SOCKET client = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(client != INVALID_SOCKET);
	ASSERT_EQ(0, ::connect(client, (struct sockaddr *)&addr, sizeof addr));

	EXPECT_TRUE(aw.wait.wait());
	EXPECT_EQ(0, aw.error);
	ASSERT_TRUE(aw.socket != INVALID_SOCKET);

	::send(client, "hi", 2, 0);
	char buf[4] = {};
	ASSERT_EQ(2, ::recv(aw.socket, buf, 2, 0));
	EXPECT_EQ(0, memcmp(buf, "hi", 2));

	::closesocket(aw.socket);
	::closesocket(client);
	acceptor.close();
	guard.reset();
	io.stop();
	worker.join();
}

TEST(async_kernel, tcp_acceptor_cancel_reports_ecanceled)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	tcp_acceptor acceptor;
	ASSERT_EQ(0, acceptor.init(executor(io)));

	ASSERT_EQ(0, acceptor.open());

	struct sockaddr_in addr;
	memset(&addr, 0, sizeof addr);
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = 0;

	ASSERT_EQ(0, acceptor.bind((struct sockaddr *)&addr, sizeof addr));
	ASSERT_EQ(0, acceptor.listen(16));

	accept_wait aw;
	ASSERT_EQ(0, acceptor.async_accept(accept_cb, &aw));
	ASSERT_EQ(0, acceptor.cancel());

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(aw.wait.wait());
	EXPECT_EQ(ECANCELED, aw.error);
	EXPECT_TRUE(aw.socket == INVALID_SOCKET);

	acceptor.close();
	guard.reset();
	io.stop();
	worker.join();
}

struct server_reply_ctx
{
	tcp_socket *server_sock;
	simple_wait wait;
	int error = -1;
};

struct server_write_ctx
{
	rw_wait wait;
};

static void server_write_cb(void *arg, async_error_code error, size_t bytes)
{
	server_write_ctx *ctx = static_cast<server_write_ctx *>(arg);
	ctx->wait.error = error_errno(error);
	ctx->wait.bytes = bytes;
	ctx->wait.wait.notify();
	delete ctx;
}

static void server_read_then_reply_cb(void *arg, async_error_code error,
									  size_t bytes)
{
	server_reply_ctx *ctx = static_cast<server_reply_ctx *>(arg);
	if (error || bytes == 0)
	{
		ctx->error = error ? error_errno(error) : EIO;
		ctx->wait.notify();
		return;
	}

	server_write_ctx *sw = new server_write_ctx;
	ctx->server_sock->async_write_some("world", 5, server_write_cb, sw);
	ctx->error = 0;
	ctx->wait.notify();
}

TEST(async_kernel, tcp_server_accept_read_reply)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	tcp_acceptor acceptor;
	ASSERT_EQ(0, acceptor.init(executor(io)));

	ASSERT_EQ(0, acceptor.open());

	struct sockaddr_in addr;
	memset(&addr, 0, sizeof addr);
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = 0;

	ASSERT_EQ(0, acceptor.bind((struct sockaddr *)&addr, sizeof addr));
	ASSERT_EQ(0, acceptor.listen(16));
	int addrlen = sizeof addr;
	ASSERT_EQ(0, ::getsockname(acceptor.native_handle(),
							   (struct sockaddr *)&addr, &addrlen));

	tcp_socket server_sock;

	ASSERT_EQ(0, server_sock.init(executor(io)));
	server_reply_ctx reply_ctx;
	reply_ctx.server_sock = &server_sock;

	accept_wait aw;
	ASSERT_EQ(0, acceptor.async_accept(accept_cb, &aw));

	std::thread worker([&io]() { io.run(); });

	SOCKET client = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(client != INVALID_SOCKET);
	ASSERT_EQ(0, ::connect(client, (struct sockaddr *)&addr, sizeof addr));

	EXPECT_TRUE(aw.wait.wait());
	ASSERT_EQ(0, aw.error);
	ASSERT_TRUE(aw.socket != INVALID_SOCKET);
	ASSERT_EQ(0, server_sock.assign(aw.socket));

	char rbuf[64] = {};
	ASSERT_EQ(0, async_read_message(&server_sock, rbuf, sizeof rbuf,
									msg_filter_5, nullptr,
									server_read_then_reply_cb, &reply_ctx));

	::send(client, "hello", 5, 0);
	EXPECT_TRUE(reply_ctx.wait.wait());
	EXPECT_EQ(0, reply_ctx.error);

	char buf[8] = {};
	ASSERT_EQ(5, ::recv(client, buf, 5, 0));
	EXPECT_EQ(0, memcmp(buf, "world", 5));

	::closesocket(client);
	acceptor.close();
	server_sock.close();
	guard.reset();
	io.stop();
	worker.join();
}

/* ---------- ssl_stream ---------- */

struct ssl_handshake_wait
{
	simple_wait wait;
	int error = -1;
};

static void ssl_handshake_cb(void *arg, async_error_code error)
{
	ssl_handshake_wait *hw = static_cast<ssl_handshake_wait *>(arg);
	hw->error = error_errno(error);
	hw->wait.notify();
}

struct ssl_accept_ctx
{
	ssl_stream *server;
	simple_wait wait;
	int error = -1;
	SOCKET socket = INVALID_SOCKET;
};

static void ssl_server_hs_cb(void *arg, async_error_code error)
{
	ssl_accept_ctx *ctx = static_cast<ssl_accept_ctx *>(arg);
	ctx->error = error_errno(error);
	ctx->wait.notify();
}

static void ssl_accept_cb(void *arg, async_error_code error, SOCKET socket)
{
	ssl_accept_ctx *ctx = static_cast<ssl_accept_ctx *>(arg);
	if (error)
	{
		ctx->error = error_errno(error);
		ctx->wait.notify();
		return;
	}

	ctx->socket = socket;
	if (ctx->server->assign(socket) != 0 ||
		ctx->server->async_handshake(ssl_server_hs_cb, ctx) != 0)
	{
		ctx->error = errno ? errno : EIO;
		ctx->wait.notify();
	}
}

TEST(async_kernel, ssl_stream_invalid_context_reports_error)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	ssl_stream stream;
	errno = 0;
	EXPECT_EQ(-1, stream.init(executor(io), nullptr, 0));
	EXPECT_EQ(EINVAL, errno);

	errno = 0;
	EXPECT_EQ(-1, stream.set_server(1));
	EXPECT_EQ(EINVAL, errno);
	EXPECT_EQ(INVALID_SOCKET, stream.native_handle());
}

TEST(async_kernel, ssl_stream_handshake_read_write)
{
	SSL_CTX *server_ctx = make_ssl_ctx(1);
	SSL_CTX *client_ctx = make_ssl_ctx(0);
	ASSERT_TRUE(server_ctx != nullptr);
	ASSERT_TRUE(client_ctx != nullptr);

	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	tcp_acceptor acceptor;
	ASSERT_EQ(0, acceptor.init(executor(io)));
	ASSERT_EQ(0, acceptor.open());

	struct sockaddr_in addr;
	memset(&addr, 0, sizeof addr);
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = 0;

	ASSERT_EQ(0, acceptor.bind((struct sockaddr *)&addr, sizeof addr));
	ASSERT_EQ(0, acceptor.listen(16));
	int addrlen = sizeof addr;
	ASSERT_EQ(0, ::getsockname(acceptor.native_handle(),
							   (struct sockaddr *)&addr, &addrlen));

	ssl_stream server;

	ASSERT_EQ(0, server.init(executor(io), server_ctx, 1));
	ssl_stream client;
	ASSERT_EQ(0, client.init(executor(io), client_ctx, 0));
	ssl_accept_ctx actx;
	actx.server = &server;

	ASSERT_EQ(0, acceptor.async_accept(ssl_accept_cb, &actx));

	std::thread worker([&io]() { io.run(); });

	SOCKET client_socket = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(client_socket != INVALID_SOCKET);
	ASSERT_EQ(0, ::connect(client_socket, (struct sockaddr *)&addr,
						   sizeof addr));
	ASSERT_EQ(0, client.assign(client_socket));

	ssl_handshake_wait client_hs;
	ASSERT_EQ(0, client.async_handshake(ssl_handshake_cb, &client_hs));

	EXPECT_TRUE(client_hs.wait.wait());
	EXPECT_EQ(0, client_hs.error);

	EXPECT_TRUE(actx.wait.wait());
	EXPECT_EQ(0, actx.error);
	ASSERT_TRUE(actx.socket != INVALID_SOCKET);

	rw_wait cw;
	ASSERT_EQ(0, client.async_write_some("ping", 4, rw_cb, &cw));
	EXPECT_TRUE(cw.wait.wait());
	EXPECT_EQ(0, cw.error);
	EXPECT_EQ(4u, cw.bytes);

	rw_wait sr;
	ASSERT_EQ(0, server.async_read_some(sr.buf, 4, rw_cb, &sr));
	EXPECT_TRUE(sr.wait.wait());
	EXPECT_EQ(0, sr.error);
	EXPECT_EQ(4u, sr.bytes);
	EXPECT_EQ(0, memcmp(sr.buf, "ping", 4));

	client.cancel();
	server.cancel();
	::closesocket(client_socket);
	::closesocket(actx.socket);
	acceptor.close();
	guard.reset();
	io.stop();
	worker.join();

	SSL_CTX_free(server_ctx);
	SSL_CTX_free(client_ctx);
}

TEST(async_kernel, ssl_stream_writev_read)
{
	SSL_CTX *server_ctx = make_ssl_ctx(1);
	SSL_CTX *client_ctx = make_ssl_ctx(0);
	ASSERT_TRUE(server_ctx != nullptr);
	ASSERT_TRUE(client_ctx != nullptr);

	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	tcp_acceptor acceptor;
	ASSERT_EQ(0, acceptor.init(executor(io)));
	ASSERT_EQ(0, acceptor.open());

	struct sockaddr_in addr;
	memset(&addr, 0, sizeof addr);
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = 0;

	ASSERT_EQ(0, acceptor.bind((struct sockaddr *)&addr, sizeof addr));
	ASSERT_EQ(0, acceptor.listen(16));
	int addrlen = sizeof addr;
	ASSERT_EQ(0, ::getsockname(acceptor.native_handle(),
							   (struct sockaddr *)&addr, &addrlen));

	ssl_stream server;

	ASSERT_EQ(0, server.init(executor(io), server_ctx, 1));
	ssl_stream client;
	ASSERT_EQ(0, client.init(executor(io), client_ctx, 0));
	ssl_accept_ctx actx;
	actx.server = &server;

	ASSERT_EQ(0, acceptor.async_accept(ssl_accept_cb, &actx));

	std::thread worker([&io]() { io.run(); });

	SOCKET client_socket = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(client_socket != INVALID_SOCKET);
	ASSERT_EQ(0, ::connect(client_socket, (struct sockaddr *)&addr,
						   sizeof addr));
	ASSERT_EQ(0, client.assign(client_socket));

	ssl_handshake_wait client_hs;
	ASSERT_EQ(0, client.async_handshake(ssl_handshake_cb, &client_hs));
	EXPECT_TRUE(client_hs.wait.wait());
	EXPECT_EQ(0, client_hs.error);

	EXPECT_TRUE(actx.wait.wait());
	EXPECT_EQ(0, actx.error);
	ASSERT_TRUE(actx.socket != INVALID_SOCKET);

	char data[] = "hello";
	struct iovec iov[2] = {{data, 2}, {data + 2, 3}};
	rw_wait cw;
	ASSERT_EQ(0, client.async_writev_some(iov, 2, rw_cb, &cw));
	EXPECT_TRUE(cw.wait.wait());
	EXPECT_EQ(0, cw.error);
	EXPECT_EQ(5u, cw.bytes);

	rw_wait sr;
	ASSERT_EQ(0, server.async_read_some(sr.buf, 5, rw_cb, &sr));
	EXPECT_TRUE(sr.wait.wait());
	EXPECT_EQ(0, sr.error);
	EXPECT_EQ(5u, sr.bytes);
	EXPECT_EQ(0, memcmp(sr.buf, "hello", 5));

	client.cancel();
	server.cancel();
	::closesocket(client_socket);
	::closesocket(actx.socket);
	acceptor.close();
	guard.reset();
	io.stop();
	worker.join();

	SSL_CTX_free(server_ctx);
	SSL_CTX_free(client_ctx);
}

TEST(async_kernel, ssl_stream_read_message_ssl)
{
	SSL_CTX *server_ctx = make_ssl_ctx(1);
	SSL_CTX *client_ctx = make_ssl_ctx(0);
	ASSERT_TRUE(server_ctx != nullptr);
	ASSERT_TRUE(client_ctx != nullptr);

	io_context io;
	ASSERT_EQ(0, io.init());
	executor_work_guard guard{executor(io)};
	tcp_acceptor acceptor;
	ASSERT_EQ(0, acceptor.init(executor(io)));
	ASSERT_EQ(0, acceptor.open());

	struct sockaddr_in addr;
	memset(&addr, 0, sizeof addr);
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = 0;

	ASSERT_EQ(0, acceptor.bind((struct sockaddr *)&addr, sizeof addr));
	ASSERT_EQ(0, acceptor.listen(16));
	int addrlen = sizeof addr;
	ASSERT_EQ(0, ::getsockname(acceptor.native_handle(),
							   (struct sockaddr *)&addr, &addrlen));

	ssl_stream server;

	ASSERT_EQ(0, server.init(executor(io), server_ctx, 1));
	ssl_stream client;
	ASSERT_EQ(0, client.init(executor(io), client_ctx, 0));
	ssl_accept_ctx actx;
	actx.server = &server;

	ASSERT_EQ(0, acceptor.async_accept(ssl_accept_cb, &actx));

	std::thread worker([&io]() { io.run(); });

	SOCKET client_socket = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(client_socket != INVALID_SOCKET);
	ASSERT_EQ(0, ::connect(client_socket, (struct sockaddr *)&addr,
						   sizeof addr));
	ASSERT_EQ(0, client.assign(client_socket));

	ssl_handshake_wait client_hs;
	ASSERT_EQ(0, client.async_handshake(ssl_handshake_cb, &client_hs));
	EXPECT_TRUE(client_hs.wait.wait());
	EXPECT_EQ(0, client_hs.error);

	EXPECT_TRUE(actx.wait.wait());
	EXPECT_EQ(0, actx.error);
	ASSERT_TRUE(actx.socket != INVALID_SOCKET);

	rw_wait sr;
	ASSERT_EQ(0, async_read_message_ssl(&server, sr.buf, sizeof sr.buf,
										msg_filter_5, nullptr, rw_cb, &sr));

	rw_wait cw;
	ASSERT_EQ(0, client.async_write_some("hello", 5, rw_cb, &cw));
	EXPECT_TRUE(cw.wait.wait());
	EXPECT_EQ(0, cw.error);
	EXPECT_EQ(5u, cw.bytes);

	EXPECT_TRUE(sr.wait.wait());
	EXPECT_EQ(0, sr.error);
	EXPECT_EQ(5u, sr.bytes);
	EXPECT_EQ(0, memcmp(sr.buf, "hello", 5));

	client.cancel();
	server.cancel();
	::closesocket(client_socket);
	::closesocket(actx.socket);
	acceptor.close();
	guard.reset();
	io.stop();
	worker.join();

	SSL_CTX_free(server_ctx);
	SSL_CTX_free(client_ctx);
}

/* ---------- win_iocp_operation ---------- */

struct test_op : win_iocp_operation
{
	int *completed;
	int *destroyed;

	test_op(int *completed_, int *destroyed_)
		: win_iocp_operation(&test_op::do_complete),
		  completed(completed_),
		  destroyed(destroyed_)
	{
	}

	static void do_complete(void *owner, win_iocp_operation *base,
							async_error_code error, size_t bytes)
	{
		(void)error;
		(void)bytes;
		test_op *self = static_cast<test_op *>(base);
		if (owner)
			++*self->completed;
		else
			++*self->destroyed;
		delete self;
	}
};

TEST(async_kernel, win_iocp_operation_on_completion_dispatches)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	int completed = 0;
	int destroyed = 0;
	test_op *op = new test_op(&completed, &destroyed);

	io.work_started();
	io.on_completion(op, async_error_code(), 42);
	io.run();

	EXPECT_EQ(1, completed);
	EXPECT_EQ(0, destroyed);
}

TEST(async_kernel, win_iocp_operation_abandon_calls_destroy)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	int completed = 0;
	int destroyed = 0;
	test_op *op = new test_op(&completed, &destroyed);

	io.work_started();
	io.post_deferred_completion(op);
	io.shutdown();

	EXPECT_EQ(0, completed);
	EXPECT_EQ(1, destroyed);
}

/* ---------- win_iocp_operation abandon semantics ---------- */

struct cleanup_count
{
	int destroyed = 0;
};

static void cleanup_fn(void *arg)
{
	++static_cast<cleanup_count *>(arg)->destroyed;
}

static void noop_routine(void *)
{
}

TEST(async_kernel, io_context_post_does_not_abandon_on_success)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	simple_wait wait;
	cleanup_count cleanup;

	struct ctx_t
	{
		simple_wait *wait;
		cleanup_count *cleanup;
	} ctx{&wait, &cleanup};

	ASSERT_EQ(0, io.post([](void *arg) {
		static_cast<ctx_t *>(arg)->wait->notify();
	}, &ctx, [](void *arg) {
		static_cast<ctx_t *>(arg)->cleanup->destroyed++;
	}));

	std::thread worker([&io]() { io.run(); });
	EXPECT_TRUE(wait.wait());
	io.stop();
	worker.join();

	EXPECT_EQ(0, cleanup.destroyed);
}

TEST(async_kernel, io_context_abandon_calls_cleanup_without_routine)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	cleanup_count cleanup;

	ASSERT_EQ(0, io.post(noop_routine, &cleanup, cleanup_fn));
	io.shutdown();

	EXPECT_EQ(1, cleanup.destroyed);
}

TEST(async_kernel, io_context_failed_post_keeps_context_ownership)
{
	io_context io;
	cleanup_count cleanup;

	EXPECT_EQ(-1, io.post(noop_routine, &cleanup, cleanup_fn));
	EXPECT_EQ(EINVAL, errno);
	EXPECT_EQ(0, cleanup.destroyed);
}

TEST(async_kernel, io_context_post_after_shutdown_keeps_context_ownership)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	ASSERT_EQ(0, io.shutdown());
	cleanup_count cleanup;

	EXPECT_EQ(-1, io.post(noop_routine, &cleanup, cleanup_fn));
	EXPECT_EQ(ECANCELED, errno);
	EXPECT_EQ(0, cleanup.destroyed);
}

struct abandon_wait_state
{
	int completed = 0;
	int abandoned = 0;
};

static void abandon_wait_cb(void *arg, async_error_code)
{
	++static_cast<abandon_wait_state *>(arg)->completed;
}

static void abandon_wait_cleanup(void *arg)
{
	++static_cast<abandon_wait_state *>(arg)->abandoned;
}

TEST(async_kernel, steady_timer_shutdown_abandons_handler)
{
	io_context io;
	ASSERT_EQ(0, io.init());
	steady_timer timer;
	ASSERT_EQ(0, timer.init(executor(io)));
	abandon_wait_state state;

	timer.expires_after(std::chrono::hours(1));
	ASSERT_EQ(0, timer.async_wait(
		abandon_wait_cb, &state, abandon_wait_cleanup));
	ASSERT_EQ(0, io.shutdown());

	EXPECT_EQ(0, state.completed);
	EXPECT_EQ(1, state.abandoned);
}
