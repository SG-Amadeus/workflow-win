class CommunicatorTestAccess;

#include "Communicator.h"
#include "../src/kernel_win/CommunicatorAsio.h"
#include <gtest/gtest.h>

#include <WinSock2.h>
#include <WS2tcpip.h>
#include <windows.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <thread>
#include <vector>

class CommunicatorTestAccess
{
public:
	static void fail_service_on_strand(Communicator *comm,
									   CommService *service, int error)
	{
		comm->fail_service(service, error);
	}

	static void fail_service(Communicator *comm, CommService *service,
							int error)
	{
		comm->fail_service(service, error);
	}

	static size_t packet_pool_outstanding(Communicator *comm)
	{
		return comm->backend_->packet_pool_outstanding();
	}

	static void set_fail_target_alloc(Communicator *comm, bool fail)
	{
		comm->test_fail_target_alloc = fail;
	}
};

namespace {

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

WSAEnv g_wsa_env;

unsigned short pick_free_port()
{
	SOCKET s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	struct sockaddr_in sin;
	int len = sizeof sin;

	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = 0;
	if (s == INVALID_SOCKET)
		return 0;
	if (bind(s, (struct sockaddr *)&sin, sizeof sin) != 0 ||
		getsockname(s, (struct sockaddr *)&sin, &len) != 0)
	{
		closesocket(s);
		return 0;
	}

	unsigned short port = ntohs(sin.sin_port);
	closesocket(s);
	return port;
}

unsigned short pick_free_port6()
{
	SOCKET s = socket(AF_INET6, SOCK_STREAM, IPPROTO_TCP);
	struct sockaddr_in6 sin6;
	int len = sizeof sin6;

	memset(&sin6, 0, sizeof sin6);
	sin6.sin6_family = AF_INET6;
	sin6.sin6_addr = in6addr_loopback;
	sin6.sin6_port = 0;
	if (s == INVALID_SOCKET)
		return 0;
	if (bind(s, (struct sockaddr *)&sin6, sizeof sin6) != 0 ||
		getsockname(s, (struct sockaddr *)&sin6, &len) != 0)
	{
		closesocket(s);
		return 0;
	}

	unsigned short port = ntohs(sin6.sin6_port);
	closesocket(s);
	return port;
}

struct TestMessageOut : public CommMessageOut
{
	int encode(struct iovec *vectors, int max) override
	{
		static char data[] = "hello";

		if (max < 1)
		{
			errno = EOVERFLOW;
			return -1;
		}

		vectors[0].iov_base = data;
		vectors[0].iov_len = sizeof data - 1;
		return 1;
	}
};

struct TestSession;

struct TestMessageIn : public CommMessageIn
{
	TestSession *owner;

	explicit TestMessageIn(TestSession *o) : owner(o) {}
	int append(const void *buf, size_t *size) override;
};

struct TestSession : public CommSession
{
	Communicator *comm;
	TestMessageOut out;
	TestMessageIn in;
	std::mutex mutex;
	std::condition_variable cv;
	bool done = false;
	int result_state = -1;
	int result_error = -1;
	bool message_out_on_handler = false;
	bool handle_on_handler = false;
	bool append_on_handler = false;
	bool fail_message_out = false;
	int receive_timeout_ms = -1;
	int message_out_calls = 0;
	int handle_calls = 0;
	DWORD message_out_tid = 0;
	DWORD handle_tid = 0;
	DWORD append_tid = 0;

	explicit TestSession(Communicator *c) : comm(c), in(this) {}

	int receive_timeout() override
	{
		return receive_timeout_ms;
	}

	CommMessageOut *message_out() override
	{
		message_out_calls++;
		message_out_on_handler = comm->is_handler_thread();
		message_out_tid = GetCurrentThreadId();
		if (fail_message_out)
			return nullptr;
		return &out;
	}

	CommMessageIn *message_in() override
	{
		return &in;
	}

	void handle(int state, int error) override
	{
		handle_calls++;
		handle_on_handler = comm->is_handler_thread();
		handle_tid = GetCurrentThreadId();
		{
			std::lock_guard<std::mutex> lock(mutex);
			result_state = state;
			result_error = error;
			done = true;
		}
		cv.notify_all();
	}

	bool wait_result(int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		return cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
						   [this] { return done; });
	}
};

int TestMessageIn::append(const void *buf, size_t *size)
{
	(void)buf;
	owner->append_on_handler = owner->comm->is_handler_thread();
	owner->append_tid = GetCurrentThreadId();
	if (*size > 0)
		*size = 1;
	return 1;
}

struct TestTarget : public CommTarget
{
	SOCKET create_connect_socket() override
	{
		return socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	}
};

struct TestServerSession;

struct TestServerMessageOut : public CommMessageOut
{
	int encode(struct iovec *vectors, int max) override
	{
		static char data[] = "world";

		if (max < 1)
		{
			errno = EOVERFLOW;
			return -1;
		}

		vectors[0].iov_base = data;
		vectors[0].iov_len = sizeof data - 1;
		return 1;
	}
};

struct TestServerMessageIn : public CommMessageIn
{
	int append(const void *buf, size_t *size) override
	{
		(void)buf;
		if (*size > 0)
			*size = 1;
		return 1;
	}
};

struct TestServerSession : public CommSession
{
	Communicator *comm;
	TestServerMessageOut out;
	TestServerMessageIn in;
	std::mutex mutex;
	std::condition_variable cv;
	bool toreply = false;
	bool success = false;
	bool new_session_on_handler = false;
	bool handle_on_handler = false;
	bool message_out_on_handler = false;
	int handle_calls = 0;
	int last_state = -1;
	int last_error = -1;
	int keep_alive_ms = 0;
	bool fail_message_out = false;
	int message_out_calls = 0;

	explicit TestServerSession(Communicator *c) : comm(c) {}

	int keep_alive_timeout() override
	{
		return keep_alive_ms;
	}

	CommMessageOut *message_out() override
	{
		message_out_calls++;
		message_out_on_handler = comm->is_handler_thread();
		if (fail_message_out)
			return nullptr;
		return &out;
	}

	CommMessageIn *message_in() override
	{
		return &in;
	}

	void handle(int state, int error) override
	{
		handle_calls++;
		handle_on_handler = comm->is_handler_thread();
		last_state = state;
		last_error = error;
		if (state == CS_STATE_TOREPLY)
		{
			std::lock_guard<std::mutex> lock(mutex);
			toreply = true;
			cv.notify_all();
		}
		else if (state == CS_STATE_SUCCESS || state == CS_STATE_ERROR)
		{
			std::lock_guard<std::mutex> lock(mutex);
			success = true;
			cv.notify_all();
		}
	}

	bool wait_toreply(int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		return cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
						   [this] { return toreply; });
	}

	bool wait_success(int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		return cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
						   [this] { return success; });
	}
};

struct TestServerService : public CommService
{
	Communicator *comm;
	std::mutex mutex;
	std::condition_variable cv;
	std::vector<TestServerSession *> sessions;
	TestServerSession *accepted = nullptr;
	bool new_session_on_handler = false;
	bool new_connection_on_handler = false;
	int unbound_calls = 0;
	bool unbound_on_handler = false;
	int stop_calls = 0;
	bool stop_on_handler = false;

	explicit TestServerService(Communicator *c) : comm(c) {}

	CommConnection *new_connection(SOCKET socket) override
	{
		(void)socket;
		new_connection_on_handler = comm->is_handler_thread();
		return new CommConnection;
	}

	CommSession *new_session(long long seq, CommConnection *conn) override
	{
		(void)seq;
		(void)conn;
		new_session_on_handler = comm->is_handler_thread();
		TestServerSession *session = new TestServerSession(comm);
		{
			std::lock_guard<std::mutex> lock(mutex);
			sessions.push_back(session);
			accepted = session;
		}
		cv.notify_all();
		return session;
	}

	void handle_stop(int error) override
	{
		(void)error;
		std::lock_guard<std::mutex> lock(mutex);
		stop_calls++;
		stop_on_handler = comm->is_handler_thread();
		cv.notify_all();
	}

	void handle_unbound() override
	{
		std::lock_guard<std::mutex> lock(mutex);
		unbound_calls++;
		unbound_on_handler = comm->is_handler_thread();
		cv.notify_all();
	}

	TestServerSession *wait_accepted(int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
					[this] { return !sessions.empty(); });
		return sessions.empty() ? nullptr : sessions[0];
	}

	TestServerSession *wait_session_at(size_t index,
									  int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
					[this, index] { return sessions.size() > index; });
		return sessions.size() > index ? sessions[index] : nullptr;
	}

	bool wait_unbound(int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		return cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
						   [this] { return unbound_calls > 0; });
	}

	bool wait_stop(int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		return cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
						   [this] { return stop_calls > 0; });
	}
};

struct ServerClient
{
	static void run(unsigned short port)
	{
		SOCKET fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
		if (fd == INVALID_SOCKET)
			return;

		struct sockaddr_in sin;
		memset(&sin, 0, sizeof sin);
		sin.sin_family = AF_INET;
		sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		sin.sin_port = htons(port);

		if (connect(fd, (struct sockaddr *)&sin, sizeof sin) == 0)
		{
			const char *req = "hello";
			send(fd, req, 5, 0);
			char buf[16];
			recv(fd, buf, sizeof buf, 0);
		}

		closesocket(fd);
	}
};


struct TestDatagramMessageIn : public CommMessageIn
{
	Communicator *comm;
	bool append_on_handler = false;
	DWORD append_tid = 0;

	explicit TestDatagramMessageIn(Communicator *c) : comm(c) {}

	int append(const void *buf, size_t *size) override
	{
		(void)buf;
		append_on_handler = comm->is_handler_thread();
		append_tid = GetCurrentThreadId();
		if (*size > 0)
			*size = 1;
		return 1;
	}
};

struct TestDatagramSession : public CommSession
{
	Communicator *comm;
	TestMessageOut out;
	TestDatagramMessageIn in;
	std::mutex mutex;
	std::condition_variable cv;
	bool toreply = false;
	bool handle_on_handler = false;
	int handle_calls = 0;
	int last_state = -1;
	int last_error = -1;

	explicit TestDatagramSession(Communicator *c) : comm(c), in(c) {}

	CommMessageOut *message_out() override
	{
		return &out;
	}

	CommMessageIn *message_in() override
	{
		return &in;
	}

	void handle(int state, int error) override
	{
		handle_on_handler = comm->is_handler_thread();
		handle_calls++;
		last_state = state;
		last_error = error;
		if (state == CS_STATE_TOREPLY)
		{
			std::lock_guard<std::mutex> lock(mutex);
			toreply = true;
			cv.notify_all();
		}
	}

	bool wait_toreply(int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		return cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
						   [this] { return toreply; });
	}
};

struct TestDatagramService : public CommService
{
	Communicator *comm;
	std::mutex mutex;
	std::condition_variable cv;
	TestDatagramSession *accepted = nullptr;
	bool new_connection_on_handler = false;
	bool new_session_on_handler = false;
	int unbound_calls = 0;
	bool unbound_on_handler = false;

	explicit TestDatagramService(Communicator *c) : comm(c) {}

	CommConnection *new_connection(SOCKET socket) override
	{
		(void)socket;
		new_connection_on_handler = comm->is_handler_thread();
		return new CommConnection;
	}

	CommSession *new_session(long long seq, CommConnection *conn) override
	{
		(void)seq;
		(void)conn;
		new_session_on_handler = comm->is_handler_thread();
		TestDatagramSession *session = new TestDatagramSession(comm);
		{
			std::lock_guard<std::mutex> lock(mutex);
			accepted = session;
		}
		cv.notify_all();
		return session;
	}

	void handle_unbound() override
	{
		std::lock_guard<std::mutex> lock(mutex);
		unbound_calls++;
		unbound_on_handler = comm->is_handler_thread();
		cv.notify_all();
	}

	bool wait_unbound(int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		return cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
						   [this] { return unbound_calls > 0; });
	}

	TestDatagramSession *wait_accepted(int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
					[this] { return accepted != nullptr; });
		return accepted;
	}
};

void send_datagram(unsigned short port, const char *data, int len)
{
	SOCKET fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if (fd == INVALID_SOCKET)
		return;

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);
	sendto(fd, data, len, 0, (struct sockaddr *)&sin, sizeof sin);
	closesocket(fd);
}

struct TestSleepSession : public SleepSession
{
	Communicator *comm;
	long long ms;
	std::mutex mutex;
	std::condition_variable cv;
	bool done = false;
	bool handle_on_handler = false;
	int handle_calls = 0;
	int state = -1;
	int error = -1;

	explicit TestSleepSession(Communicator *c, long long value_ms)
		: comm(c), ms(value_ms)
	{
	}

	int duration(struct timespec *value) override
	{
		value->tv_sec = ms / 1000;
		value->tv_nsec = (ms % 1000) * 1000000;
		return 0;
	}

	void handle(int state, int error) override
	{
		handle_calls++;
		handle_on_handler = comm->is_handler_thread();
		{
			std::lock_guard<std::mutex> lock(mutex);
			this->state = state;
			this->error = error;
			done = true;
		}
		cv.notify_all();
	}

	bool wait_result(int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		return cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
						   [this] { return done; });
	}
};

struct BlockingServerSession : public CommSession
{
	Communicator *comm;
	TestServerMessageOut out;
	TestServerMessageIn in;
	std::mutex mutex;
	std::condition_variable cv;
	bool handle_started = false;
	bool release = false;
	bool toreply = false;
	bool handle_on_handler = false;

	explicit BlockingServerSession(Communicator *c) : comm(c) {}

	CommMessageOut *message_out() override
	{
		return &out;
	}

	CommMessageIn *message_in() override
	{
		return &in;
	}

	void handle(int state, int error) override
	{
		handle_on_handler = comm->is_handler_thread();
		if (state == CS_STATE_TOREPLY)
		{
			{
				std::lock_guard<std::mutex> lock(mutex);
				handle_started = true;
			}
			cv.notify_all();

			std::unique_lock<std::mutex> lock(mutex);
			cv.wait(lock, [this] { return release; });
			toreply = true;
			cv.notify_all();
		}
	}

	bool wait_started(int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		return cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
						   [this] { return handle_started; });
	}

	void release_handle()
	{
		std::lock_guard<std::mutex> lock(mutex);
		release = true;
		cv.notify_all();
	}

	bool wait_toreply(int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		return cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
						   [this] { return toreply; });
	}
};

struct BlockingServerService : public CommService
{
	Communicator *comm;
	std::mutex mutex;
	std::condition_variable cv;
	std::vector<BlockingServerSession *> sessions;
	int new_session_count = 0;
	int unbound_calls = 0;
	bool unbound_on_handler = false;

	explicit BlockingServerService(Communicator *c) : comm(c) {}

	CommConnection *new_connection(SOCKET socket) override
	{
		(void)socket;
		return new CommConnection;
	}

	CommSession *new_session(long long seq, CommConnection *conn) override
	{
		(void)seq;
		(void)conn;
		BlockingServerSession *session = new BlockingServerSession(comm);
		{
			std::lock_guard<std::mutex> lock(mutex);
			sessions.push_back(session);
			new_session_count++;
		}
		cv.notify_all();
		return session;
	}

	void handle_unbound() override
	{
		std::lock_guard<std::mutex> lock(mutex);
		unbound_calls++;
		unbound_on_handler = comm->is_handler_thread();
		cv.notify_all();
	}

	BlockingServerSession *wait_session(int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
					[this] { return !sessions.empty(); });
		return sessions.empty() ? nullptr : sessions[0];
	}

	BlockingServerSession *wait_session_at(size_t index,
										  int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
					[this, index] { return sessions.size() > index; });
		return sessions.size() > index ? sessions[index] : nullptr;
	}

	bool wait_unbound(int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		return cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
						   [this] { return unbound_calls > 0; });
	}
};

void send_only_client(unsigned short port)
{
	SOCKET fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (fd == INVALID_SOCKET)
		return;

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	if (connect(fd, (struct sockaddr *)&sin, sizeof sin) == 0)
	{
		const char *req = "hello";
		send(fd, req, 5, 0);
		Sleep(200);
	}

	closesocket(fd);
}

void stay_open_client(unsigned short port)
{
	SOCKET fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (fd == INVALID_SOCKET)
		return;

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	if (connect(fd, (struct sockaddr *)&sin, sizeof sin) == 0)
	{
		const char *req = "hello";
		char buf[16];
		send(fd, req, 5, 0);
		if (recv(fd, buf, sizeof buf, 0) > 0)
			Sleep(300);
	}

	closesocket(fd);
}

void keepalive_client(unsigned short port)
{
	SOCKET fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (fd == INVALID_SOCKET)
		return;

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	if (connect(fd, (struct sockaddr *)&sin, sizeof sin) == 0)
	{
		const char *req = "hello";
		char buf[16];
		send(fd, req, 5, 0);
		if (recv(fd, buf, sizeof buf, 0) > 0)
			send(fd, req, 5, 0);
		recv(fd, buf, sizeof buf, 0);
	}

	closesocket(fd);
}

void delayed_send_client(unsigned short port, int delay_ms)
{
	SOCKET fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (fd == INVALID_SOCKET)
		return;

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	if (connect(fd, (struct sockaddr *)&sin, sizeof sin) == 0)
	{
		Sleep(delay_ms);
		const char *req = "hello";
		send(fd, req, 5, 0);
	}

	closesocket(fd);
}

void send_and_close_client(unsigned short port)
{
	SOCKET fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (fd == INVALID_SOCKET)
		return;

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	if (connect(fd, (struct sockaddr *)&sin, sizeof sin) == 0)
	{
		const char *req = "hello";
		send(fd, req, 5, 0);
		closesocket(fd);
	}
	else
	{
		closesocket(fd);
	}
}

void connect_only_client(unsigned short port)
{
	SOCKET fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (fd == INVALID_SOCKET)
		return;

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	if (connect(fd, (struct sockaddr *)&sin, sizeof sin) == 0)
		Sleep(500);

	closesocket(fd);
}

void connect_only_client6(unsigned short port)
{
	SOCKET fd = socket(AF_INET6, SOCK_STREAM, IPPROTO_TCP);
	if (fd == INVALID_SOCKET)
		return;

	struct sockaddr_in6 sin6;
	memset(&sin6, 0, sizeof sin6);
	sin6.sin6_family = AF_INET6;
	sin6.sin6_addr = in6addr_loopback;
	sin6.sin6_port = htons(port);

	if (connect(fd, (struct sockaddr *)&sin6, sizeof sin6) == 0)
		Sleep(500);

	closesocket(fd);
}

struct LoopbackListener
{
	SOCKET listen_fd = INVALID_SOCKET;
	unsigned short port = 0;
	int delay_ms = 0;
	std::thread thread;
	bool started = false;

	~LoopbackListener()
	{
		if (listen_fd != INVALID_SOCKET)
			closesocket(listen_fd);
		if (thread.joinable())
			thread.join();
	}

	bool start()
	{
		listen_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
		if (listen_fd == INVALID_SOCKET)
			return false;

		struct sockaddr_in sin;
		memset(&sin, 0, sizeof sin);
		sin.sin_family = AF_INET;
		sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		sin.sin_port = 0;

		if (bind(listen_fd, (struct sockaddr *)&sin, sizeof sin) != 0)
			return false;
		if (listen(listen_fd, 1) != 0)
			return false;

		int len = sizeof sin;
		if (getsockname(listen_fd, (struct sockaddr *)&sin, &len) != 0)
			return false;

		port = ntohs(sin.sin_port);
		thread = std::thread([this] { run(); });
		started = true;
		return true;
	}

	void run()
	{
		SOCKET client = accept(listen_fd, NULL, NULL);
		if (client == INVALID_SOCKET)
			return;

		char buf[16];
		int n = recv(client, buf, sizeof buf, 0);
		(void)n;
		if (delay_ms > 0)
			Sleep(delay_ms);
		const char *resp = "OK";
		send(client, resp, 2, 0);
		closesocket(client);
		closesocket(listen_fd);
		listen_fd = INVALID_SOCKET;
	}
};

TEST(communicator, client_message_out_and_handle_run_on_handler_thread)
{
	LoopbackListener listener;
	ASSERT_TRUE(listener.start());

	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(listener.port);

	TestTarget target;
	ASSERT_EQ(target.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);

	TestSession session(&comm);
	ASSERT_EQ(comm.request(&session, &target), 0);
	ASSERT_TRUE(session.wait_result());

	EXPECT_EQ(session.result_state, CS_STATE_SUCCESS);
	EXPECT_EQ(session.result_error, 0);
	EXPECT_TRUE(session.message_out_on_handler);
	EXPECT_TRUE(session.handle_on_handler);
	EXPECT_FALSE(session.append_on_handler);
	EXPECT_NE(session.message_out_tid, GetCurrentThreadId());
	EXPECT_NE(session.handle_tid, GetCurrentThreadId());
	EXPECT_NE(session.append_tid, GetCurrentThreadId());

	comm.deinit();
	target.deinit();
	listener.thread.join();
	listener.started = false;
}

TEST(communicator, client_prepare_failure_single_terminal_result)
{
	LoopbackListener listener;
	ASSERT_TRUE(listener.start());

	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(listener.port);

	TestTarget target;
	ASSERT_EQ(target.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);

	TestSession session(&comm);
	session.fail_message_out = true;
	ASSERT_EQ(comm.request(&session, &target), 0);
	ASSERT_TRUE(session.wait_result());

	EXPECT_EQ(session.message_out_calls, 1);
	EXPECT_EQ(session.handle_calls, 1);
	EXPECT_EQ(session.result_state, CS_STATE_ERROR);
	EXPECT_EQ(session.result_error, ENOSYS);
	EXPECT_TRUE(session.message_out_on_handler);
	EXPECT_TRUE(session.handle_on_handler);

	comm.deinit();
	target.deinit();
	listener.thread.join();
	listener.started = false;
}

TEST(communicator, client_receive_timeout_single_terminal)
{
	LoopbackListener listener;
	listener.delay_ms = 500;
	ASSERT_TRUE(listener.start());

	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(listener.port);

	TestTarget target;
	ASSERT_EQ(target.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);

	TestSession session(&comm);
	session.receive_timeout_ms = 100;
	ASSERT_EQ(comm.request(&session, &target), 0);
	ASSERT_TRUE(session.wait_result());

	EXPECT_EQ(session.result_state, CS_STATE_ERROR);
	EXPECT_NE(session.result_error, 0);
	EXPECT_EQ(session.handle_calls, 1);

	Sleep(600);
	EXPECT_EQ(session.handle_calls, 1);

	comm.deinit();
	target.deinit();
	listener.thread.join();
	listener.started = false;
}

TEST(communicator, server_new_session_and_handle_run_on_handler_thread)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	TestServerService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);
	ASSERT_EQ(comm.bind(&service), 0);

	std::thread client(ServerClient::run, port);
	TestServerSession *session = service.wait_accepted();
	ASSERT_TRUE(session != nullptr);
	ASSERT_TRUE(session->wait_toreply());

	EXPECT_TRUE(service.new_connection_on_handler);
	EXPECT_TRUE(service.new_session_on_handler);
	EXPECT_TRUE(session->handle_on_handler);
	EXPECT_EQ(session->last_state, CS_STATE_TOREPLY);

	ASSERT_EQ(comm.reply(session), 0);
	ASSERT_TRUE(session->wait_success());
	EXPECT_TRUE(session->handle_on_handler);
	delete session;

	comm.unbind(&service);
	client.join();
	ASSERT_TRUE(service.wait_unbound());
	service.deinit();
	comm.deinit();
}

TEST(communicator, server_read_timeout_single_error)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	TestServerService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  100, 200), 0);
	ASSERT_EQ(comm.bind(&service), 0);

	std::thread client(connect_only_client, port);
	TestServerSession *session = service.wait_accepted();
	ASSERT_TRUE(session != nullptr);
	ASSERT_TRUE(session->wait_success(5000));

	EXPECT_EQ(session->handle_calls, 1);
	EXPECT_EQ(session->last_state, CS_STATE_ERROR);
	EXPECT_EQ(session->last_error, ETIMEDOUT);
	EXPECT_TRUE(session->handle_on_handler);

	comm.unbind(&service);
	client.join();
	ASSERT_TRUE(service.wait_unbound());
	delete session;
	service.deinit();
	comm.deinit();
}

TEST(communicator, late_data_after_server_read_timeout_single_error)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	TestServerService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  100, 200), 0);
	ASSERT_EQ(comm.bind(&service), 0);

	std::thread client(delayed_send_client, port, 300);
	TestServerSession *session = service.wait_accepted();
	ASSERT_TRUE(session != nullptr);
	ASSERT_TRUE(session->wait_success(5000));

	EXPECT_EQ(session->handle_calls, 1);
	EXPECT_EQ(session->last_state, CS_STATE_ERROR);
	EXPECT_EQ(session->last_error, ETIMEDOUT);

	/* Late data arrives after the timeout fired; it must not produce a
	 * second terminal callback. */
	Sleep(400);
	EXPECT_EQ(session->handle_calls, 1);

	comm.unbind(&service);
	client.join();
	ASSERT_TRUE(service.wait_unbound());
	delete session;
	service.deinit();
	comm.deinit();
}

TEST(communicator, ipv6_accept_runs_on_handler_thread)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port6();
	ASSERT_NE(port, 0);

	struct sockaddr_in6 sin6;
	memset(&sin6, 0, sizeof sin6);
	sin6.sin6_family = AF_INET6;
	sin6.sin6_addr = in6addr_loopback;
	sin6.sin6_port = htons(port);

	TestServerService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin6, sizeof sin6,
						  1000, 1000), 0);
	ASSERT_EQ(comm.bind(&service), 0);

	std::thread client(connect_only_client6, port);
	TestServerSession *session = service.wait_accepted();
	ASSERT_TRUE(session != nullptr);
	ASSERT_TRUE(session->wait_success(5000));
	EXPECT_TRUE(session->handle_on_handler);

	comm.unbind(&service);
	client.join();
	ASSERT_TRUE(service.wait_unbound());
	delete session;
	service.deinit();
	comm.deinit();
}

TEST(communicator, unbind_while_pending_handler_close_drains)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	BlockingServerService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);
	ASSERT_EQ(comm.bind(&service), 0);

	std::thread client(send_only_client, port);
	BlockingServerSession *session = service.wait_session();
	ASSERT_TRUE(session != nullptr);
	ASSERT_TRUE(session->wait_started());

	/* The handler is blocked inside handle(TOREPLY).  Unbinding must close
	 * the service producer without waiting for the handler; the pending
	 * connection callback then drains after the handler releases. */
	comm.unbind(&service);

	session->release_handle();
	ASSERT_TRUE(session->wait_toreply());

	client.join();
	ASSERT_TRUE(service.wait_unbound());
	delete session;
	service.deinit();
	comm.deinit();
}

TEST(communicator, sleep_completion_runs_on_handler_thread)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	TestSleepSession session(&comm, 0);
	ASSERT_EQ(comm.sleep(&session), 0);
	ASSERT_TRUE(session.wait_result());

	EXPECT_TRUE(session.handle_on_handler);
	EXPECT_EQ(session.state, SS_STATE_COMPLETE);
	EXPECT_EQ(session.error, 0);

	comm.deinit();
}

TEST(communicator, unsleep_completion_runs_on_handler_thread)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	TestSleepSession session(&comm, 10000);
	ASSERT_EQ(comm.sleep(&session), 0);
	ASSERT_EQ(comm.unsleep(&session), 0);
	ASSERT_TRUE(session.wait_result());

	EXPECT_TRUE(session.handle_on_handler);
	EXPECT_EQ(session.state, SS_STATE_ERROR);
	EXPECT_EQ(session.error, ECANCELED);

	comm.deinit();
}

TEST(communicator, sleep_expiry_unsleep_race_exactly_one_handle)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	for (int i = 0; i < 200; i++)
	{
		TestSleepSession session(&comm, 0);
		ASSERT_EQ(comm.sleep(&session), 0);

		if (i % 2)
			comm.unsleep(&session); /* may lose to expiry; ENOENT is fine */

		ASSERT_TRUE(session.wait_result());
		EXPECT_EQ(session.handle_calls, 1);
		EXPECT_TRUE(session.handle_on_handler);
	}

	comm.deinit();
}

TEST(communicator, unbound_runs_on_handler_thread_exactly_once)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	TestServerService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);
	ASSERT_EQ(comm.bind(&service), 0);

	comm.unbind(&service);
	ASSERT_TRUE(service.wait_unbound());

	EXPECT_EQ(service.unbound_calls, 1);
	EXPECT_TRUE(service.unbound_on_handler);

	service.deinit();
	comm.deinit();
}

TEST(communicator, service_stop_runs_on_handler_exactly_once)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	TestServerService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);
	ASSERT_EQ(comm.bind(&service), 0);

	/* Inject two failures. Only the first OPEN->STOPPING transition may
	 * deliver handle_stop; the second must be suppressed. */
	CommunicatorTestAccess::fail_service(&comm, &service, EIO);
	CommunicatorTestAccess::fail_service(&comm, &service, EIO);

	ASSERT_TRUE(service.wait_stop());
	Sleep(100);
	EXPECT_EQ(service.stop_calls, 1);
	EXPECT_TRUE(service.stop_on_handler);

	comm.unbind(&service);
	ASSERT_TRUE(service.wait_unbound());
	EXPECT_EQ(service.unbound_calls, 1);

	service.deinit();
	comm.deinit();
}

TEST(communicator, unbind_drains_queued_accept_admission)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	BlockingServerService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);
	ASSERT_EQ(comm.bind(&service), 0);

	std::thread client1(send_only_client, port);
	BlockingServerSession *first = service.wait_session();
	ASSERT_TRUE(first != nullptr);
	ASSERT_TRUE(first->wait_started());

	/* The only handler thread is now blocked in the first session's
	 * handle().  A second accept can complete on the ASIO side and queue
	 * ACCEPT_READY, but cannot be admitted until the handler is free. */
	std::thread client2(send_only_client, port);
	Sleep(100);

	comm.unbind(&service);
	first->release_handle();
	ASSERT_TRUE(first->wait_toreply());
	ASSERT_EQ(comm.reply(first), 0);

	/* The queued accept must be admitted even after unbind.  Release the
	 * second session too so the single handler thread can finish. */
	BlockingServerSession *second = service.wait_session_at(1);
	ASSERT_TRUE(second != nullptr);
	ASSERT_TRUE(second->wait_started());
	second->release_handle();
	ASSERT_TRUE(second->wait_toreply());
	ASSERT_EQ(comm.reply(second), 0);

	client1.join();
	client2.join();

	/* Linux semantics: an accept that already completed before unbind is
	 * drained normally; unbind only stops new producers. */
	EXPECT_EQ(service.new_session_count, 2);
	EXPECT_TRUE(first->handle_on_handler);

	delete first;
	delete second;
	service.deinit();
	comm.deinit();
}

TEST(communicator, datagram_admission_runs_on_handler_thread)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	TestDatagramService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);
	service.set_reliable(0);
	ASSERT_EQ(comm.bind(&service), 0);

	std::thread sender(send_datagram, port, "hello", 5);
	TestDatagramSession *session = service.wait_accepted();
	ASSERT_TRUE(session != nullptr);
	ASSERT_TRUE(session->wait_toreply());

	EXPECT_TRUE(service.new_connection_on_handler);
	EXPECT_TRUE(service.new_session_on_handler);
	EXPECT_TRUE(session->in.append_on_handler);
	EXPECT_TRUE(session->handle_on_handler);
	EXPECT_EQ(session->last_state, CS_STATE_TOREPLY);

	ASSERT_EQ(comm.reply(session), 0);
	delete session;

	comm.unbind(&service);
	sender.join();
	service.deinit();
	comm.deinit();
}

TEST(communicator, unbind_while_udp_reply_waiting)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	TestDatagramService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);
	service.set_reliable(0);
	ASSERT_EQ(comm.bind(&service), 0);

	std::thread sender(send_datagram, port, "hello", 5);
	TestDatagramSession *session = service.wait_accepted();
	ASSERT_TRUE(session != nullptr);
	ASSERT_TRUE(session->wait_toreply());

	std::atomic<int> reply_ret(-999);
	std::thread reply_thread([&] {
		reply_ret.store(comm.reply(session));
	});

	/* Let the reply enter the synchronous service-strand send path, then
	 * race unbind against it.  The command ref must keep ServiceTransport
	 * alive until the strand lambda has written the result. */
	Sleep(20);
	comm.unbind(&service);
	reply_thread.join();

	EXPECT_TRUE(reply_ret.load() == 0 || reply_ret.load() == -1);
	EXPECT_TRUE(service.wait_unbound());

	delete session;
	sender.join();
	service.deinit();
	comm.deinit();
}

TEST(communicator, udp_reply_after_service_close)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	TestDatagramService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);
	service.set_reliable(0);
	ASSERT_EQ(comm.bind(&service), 0);

	std::thread sender(send_datagram, port, "hello", 5);
	TestDatagramSession *session = service.wait_accepted();
	ASSERT_TRUE(session != nullptr);
	ASSERT_TRUE(session->wait_toreply());

	comm.unbind(&service);
	ASSERT_TRUE(service.wait_unbound());

	/* The service transport has retired; reply must fail cleanly instead
	 * of dereferencing a stale transport pointer. */
	EXPECT_LT(comm.reply(session), 0);

	delete session;
	sender.join();
	service.deinit();
	comm.deinit();
}

struct FragmentedServerMessageIn : public CommMessageIn
{
	struct FragmentedServerSession *session;

	explicit FragmentedServerMessageIn(FragmentedServerSession *s)
		: session(s)
	{
	}

	int append(const void *buf, size_t *size) override;
};

struct FragmentedServerSession : public CommSession
{
	Communicator *comm;
	FragmentedServerMessageIn in;
	std::mutex mutex;
	std::condition_variable cv;
	int append_calls = 0;
	bool toreply = false;
	bool handle_on_handler = false;
	int last_state = -1;

	explicit FragmentedServerSession(Communicator *c)
		: comm(c), in(this)
	{
	}

	CommMessageOut *message_out() override
	{
		return NULL;
	}

	CommMessageIn *message_in() override
	{
		return &in;
	}

	void handle(int state, int error) override
	{
		(void)error;
		handle_on_handler = comm->is_handler_thread();
		last_state = state;
		if (state == CS_STATE_TOREPLY)
		{
			std::lock_guard<std::mutex> lock(mutex);
			toreply = true;
			cv.notify_all();
		}
	}

	void notify_append()
	{
		std::lock_guard<std::mutex> lock(mutex);
		append_calls++;
		cv.notify_all();
	}

	bool wait_append_count(int count, int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		return cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
						   [this, count] { return append_calls >= count; });
	}

	bool wait_toreply(int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		return cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
						   [this] { return toreply; });
	}
};

int FragmentedServerMessageIn::append(const void *buf, size_t *size)
{
	(void)buf;
	session->notify_append();
	if (session->append_calls == 1)
	{
		/* Partial read: parser says "need more data".  The backend must
		 * keep reading on the same transport operation. */
		*size = 0;
		return 0;
	}

	if (*size > 0)
		*size = 1;
	return 1;
}

struct FragmentedServerService : public CommService
{
	Communicator *comm;
	std::mutex mutex;
	std::condition_variable cv;
	FragmentedServerSession *accepted = nullptr;

	explicit FragmentedServerService(Communicator *c) : comm(c) {}

	CommConnection *new_connection(SOCKET socket) override
	{
		(void)socket;
		return new CommConnection;
	}

	CommSession *new_session(long long seq, CommConnection *conn) override
	{
		(void)seq;
		(void)conn;
		FragmentedServerSession *session = new FragmentedServerSession(comm);
		{
			std::lock_guard<std::mutex> lock(mutex);
			accepted = session;
		}
		cv.notify_all();
		return session;
	}

	void handle_unbound() override
	{
	}

	FragmentedServerSession *wait_accepted(int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
					[this] { return accepted != nullptr; });
		return accepted;
	}
};

static void send_fragmented_request(unsigned short port)
{
	SOCKET fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (fd == INVALID_SOCKET)
		return;

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	if (connect(fd, (struct sockaddr *)&sin, sizeof sin) == 0)
	{
		send(fd, "he", 2, 0);
		Sleep(100);
		send(fd, "llo", 3, 0);
		char buf[16];
		recv(fd, buf, sizeof buf, 0);
	}

	closesocket(fd);
}

TEST(communicator, udp_double_reply_fails_without_second_handle)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	TestDatagramService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);
	service.set_reliable(0);
	ASSERT_EQ(comm.bind(&service), 0);

	std::thread sender(send_datagram, port, "hello", 5);
	TestDatagramSession *session = service.wait_accepted();
	ASSERT_TRUE(session != nullptr);
	ASSERT_TRUE(session->wait_toreply());

	ASSERT_EQ(comm.reply(session), 0);
	int after_first = session->handle_calls;
	EXPECT_GT(after_first, 0);

	EXPECT_LT(comm.reply(session), 0);
	Sleep(100);
	EXPECT_EQ(session->handle_calls, after_first);

	delete session;
	sender.join();
	comm.unbind(&service);
	ASSERT_TRUE(service.wait_unbound());
	service.deinit();
	comm.deinit();
}

TEST(communicator, datagram_target_alloc_failure)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	TestDatagramService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);
	service.set_reliable(0);
	ASSERT_EQ(comm.bind(&service), 0);

	/* Force CommServiceTarget allocation failure.  The packet must be
	 * returned and the event acknowledged; the service must stay usable. */
	CommunicatorTestAccess::set_fail_target_alloc(&comm, true);
	std::thread sender1(send_datagram, port, "bad", 3);
	sender1.join();
	EXPECT_TRUE(service.wait_accepted(200) == nullptr);

	CommunicatorTestAccess::set_fail_target_alloc(&comm, false);
	std::thread sender2(send_datagram, port, "ok", 2);
	TestDatagramSession *session = service.wait_accepted();
	ASSERT_TRUE(session != nullptr);
	ASSERT_TRUE(session->wait_toreply());

	delete session;
	sender2.join();

	comm.unbind(&service);
	ASSERT_TRUE(service.wait_unbound());
	service.deinit();
	comm.deinit();
}

TEST(communicator, cancelled_receive_returns_packet_buffer)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	TestDatagramService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);
	service.set_reliable(0);
	ASSERT_EQ(comm.bind(&service), 0);

	std::thread sender(send_datagram, port, "hello", 5);
	TestDatagramSession *session = service.wait_accepted();
	ASSERT_TRUE(session != nullptr);
	ASSERT_TRUE(session->wait_toreply());

	/* After admission, the datagram slot is re-armed with a fresh packet
	 * buffer.  Unbind must cancel that receive and return the buffer. */
	size_t before = CommunicatorTestAccess::packet_pool_outstanding(&comm);
	EXPECT_GT(before, 0u);

	comm.unbind(&service);
	ASSERT_TRUE(service.wait_unbound());

	size_t after = CommunicatorTestAccess::packet_pool_outstanding(&comm);
	EXPECT_EQ(after, 0u);

	delete session;
	sender.join();
	service.deinit();
	comm.deinit();
}

TEST(communicator, client_close_after_reply_no_second_handle)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	TestServerService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);
	ASSERT_EQ(comm.bind(&service), 0);

	std::thread client(ServerClient::run, port);
	TestServerSession *session = service.wait_accepted();
	ASSERT_TRUE(session != nullptr);
	ASSERT_TRUE(session->wait_toreply());

	ASSERT_EQ(comm.reply(session), 0);
	ASSERT_TRUE(session->wait_success());
	client.join();

	int after = session->handle_calls;
	EXPECT_GT(after, 1); /* TOREPLY + terminal SUCCESS */
	/* Client close completion must not re-enter the already-finished
	 * session. */
	Sleep(300);
	EXPECT_EQ(session->handle_calls, after);
	EXPECT_EQ(session->last_state, CS_STATE_SUCCESS);

	delete session;
	comm.unbind(&service);
	ASSERT_TRUE(service.wait_unbound());
	service.deinit();
	comm.deinit();
}

TEST(communicator, server_keepalive_timeout_no_second_handle)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	TestServerService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);
	ASSERT_EQ(comm.bind(&service), 0);

	std::thread client(ServerClient::run, port);
	TestServerSession *session = service.wait_accepted();
	ASSERT_TRUE(session != nullptr);
	ASSERT_TRUE(session->wait_toreply());

	session->keep_alive_ms = 100;
	ASSERT_EQ(comm.reply(session), 0);
	ASSERT_TRUE(session->wait_success());
	client.join();

	int after = session->handle_calls;
	EXPECT_GT(after, 1); /* TOREPLY + terminal SUCCESS */
	/* Keepalive timeout retires the idle connection; it must not call the
	 * completed session's handle() a second time. */
	Sleep(300);
	EXPECT_EQ(session->handle_calls, after);
	EXPECT_EQ(session->last_state, CS_STATE_SUCCESS);

	delete session;
	comm.unbind(&service);
	ASSERT_TRUE(service.wait_unbound());
	service.deinit();
	comm.deinit();
}

TEST(communicator, server_keepalive_timeout_idle_client_no_second_handle)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	TestServerService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);
	ASSERT_EQ(comm.bind(&service), 0);

	std::thread client(stay_open_client, port);
	TestServerSession *session = service.wait_accepted();
	ASSERT_TRUE(session != nullptr);
	ASSERT_TRUE(session->wait_toreply());

	session->keep_alive_ms = 100;
	ASSERT_EQ(comm.reply(session), 0);
	ASSERT_TRUE(session->wait_success());
	int after = session->handle_calls;
	EXPECT_GT(after, 1); /* TOREPLY + terminal SUCCESS */

	/* The client stays open; only the server-side keepalive timer can
	 * retire the idle connection. */
	Sleep(400);
	EXPECT_EQ(session->handle_calls, after);

	client.join();
	delete session;
	comm.unbind(&service);
	ASSERT_TRUE(service.wait_unbound());
	service.deinit();
	comm.deinit();
}

TEST(communicator, server_keepalive_second_request_gets_new_session)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	TestServerService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);
	ASSERT_EQ(comm.bind(&service), 0);

	std::thread client(keepalive_client, port);
	TestServerSession *session1 = service.wait_accepted();
	ASSERT_TRUE(session1 != nullptr);
	ASSERT_TRUE(session1->wait_toreply());

	session1->keep_alive_ms = 5000;
	ASSERT_EQ(comm.reply(session1), 0);
	ASSERT_TRUE(session1->wait_success());

	TestServerSession *session2 = service.wait_session_at(1);
	ASSERT_TRUE(session2 != nullptr);
	ASSERT_TRUE(session2->wait_toreply());
	ASSERT_EQ(comm.reply(session2), 0);
	ASSERT_TRUE(session2->wait_success());

	client.join();

	delete session1;
	delete session2;
	comm.unbind(&service);
	ASSERT_TRUE(service.wait_unbound());
	service.deinit();
	comm.deinit();
}

TEST(communicator, double_reply_fails_without_second_handle)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	TestServerService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);
	ASSERT_EQ(comm.bind(&service), 0);

	std::thread client(ServerClient::run, port);
	TestServerSession *session = service.wait_accepted();
	ASSERT_TRUE(session != nullptr);
	ASSERT_TRUE(session->wait_toreply());

	ASSERT_EQ(comm.reply(session), 0);
	ASSERT_TRUE(session->wait_success());
	int after = session->handle_calls;
	EXPECT_GT(after, 1); /* TOREPLY + terminal SUCCESS */
	EXPECT_LT(comm.reply(session), 0);

	Sleep(200);
	EXPECT_EQ(session->handle_calls, after);

	delete session;
	client.join();
	comm.unbind(&service);
	ASSERT_TRUE(service.wait_unbound());
	service.deinit();
	comm.deinit();
}

TEST(communicator, client_close_before_reply_single_error)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	TestServerService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);
	ASSERT_EQ(comm.bind(&service), 0);

	std::thread client(send_and_close_client, port);
	TestServerSession *session = service.wait_accepted();
	ASSERT_TRUE(session != nullptr);
	ASSERT_TRUE(session->wait_toreply());

	ASSERT_EQ(comm.reply(session), 0);
	ASSERT_TRUE(session->wait_success());
	client.join();

	int after = session->handle_calls;
	EXPECT_GT(after, 1);
	Sleep(300);
	EXPECT_EQ(session->handle_calls, after);

	delete session;
	comm.unbind(&service);
	ASSERT_TRUE(service.wait_unbound());
	service.deinit();
	comm.deinit();
}

TEST(communicator, server_message_out_failure_single_error)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	TestServerService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);
	ASSERT_EQ(comm.bind(&service), 0);

	std::thread client(send_and_close_client, port);
	TestServerSession *session = service.wait_accepted();
	ASSERT_TRUE(session != nullptr);
	ASSERT_TRUE(session->wait_toreply());

	session->fail_message_out = true;
	EXPECT_LT(comm.reply(session), 0);
	client.join();

	/* reply() rejects a null message_out synchronously; it must not
	 * deliver a second terminal handle later. */
	Sleep(100);
	EXPECT_EQ(session->handle_calls, 1);

	delete session;
	comm.unbind(&service);
	ASSERT_TRUE(service.wait_unbound());
	service.deinit();
	comm.deinit();
}

TEST(communicator, unbind_while_keepalive_parked_connection_no_second_handle)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	TestServerService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);
	ASSERT_EQ(comm.bind(&service), 0);

	std::thread client(ServerClient::run, port);
	TestServerSession *session = service.wait_accepted();
	ASSERT_TRUE(session != nullptr);
	ASSERT_TRUE(session->wait_toreply());

	session->keep_alive_ms = 10000;
	ASSERT_EQ(comm.reply(session), 0);
	ASSERT_TRUE(session->wait_success());
	client.join();

	int after = session->handle_calls;
	EXPECT_GT(after, 1);

	comm.unbind(&service);
	ASSERT_TRUE(service.wait_unbound());
	Sleep(100);
	EXPECT_EQ(session->handle_calls, after);

	delete session;
	service.deinit();
	comm.deinit();
}

TEST(communicator, stale_operation_callback_is_ignored)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	TestServerService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  100, 100), 0);
	ASSERT_EQ(comm.bind(&service), 0);

	std::thread client(ServerClient::run, port);
	TestServerSession *session = service.wait_accepted();
	ASSERT_TRUE(session != nullptr);
	ASSERT_TRUE(session->wait_toreply());

	ASSERT_EQ(comm.reply(session), 0);
	ASSERT_TRUE(session->wait_success());
	int after = session->handle_calls;
	EXPECT_GT(after, 1); /* TOREPLY + terminal SUCCESS */

	/* The connection may still have a timer/completion in flight after the
	 * reply.  A stale callback must not produce a second session handle. */
	Sleep(300);
	EXPECT_EQ(session->handle_calls, after);
	EXPECT_EQ(session->last_state, CS_STATE_SUCCESS);

	delete session;
	client.join();

	comm.unbind(&service);
	ASSERT_TRUE(service.wait_unbound());
	service.deinit();
	comm.deinit();
}

TEST(communicator, fragmented_parser_renew_timeout)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	FragmentedServerService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);
	ASSERT_EQ(comm.bind(&service), 0);

	std::thread client(send_fragmented_request, port);
	FragmentedServerSession *session = service.wait_accepted();
	ASSERT_TRUE(session != nullptr);

	/* The first two bytes produce a parser ret==0; the read must be renewed
	 * on the transport without publishing a second session or dropping the
	 * operation lease. */
	ASSERT_TRUE(session->wait_append_count(1));
	ASSERT_FALSE(session->toreply);
	ASSERT_TRUE(session->wait_toreply());
	ASSERT_EQ(session->append_calls, 2);
	EXPECT_TRUE(session->handle_on_handler);

	delete session;
	client.join();

	comm.unbind(&service);
	service.deinit();
	comm.deinit();
}

TEST(communicator, multiple_handler_threads_server_reply_single_terminal)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 2), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	TestServerService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);
	ASSERT_EQ(comm.bind(&service), 0);

	std::thread client(ServerClient::run, port);
	TestServerSession *session = service.wait_accepted();
	ASSERT_TRUE(session != nullptr);
	ASSERT_TRUE(session->wait_toreply());

	ASSERT_EQ(comm.reply(session), 0);
	ASSERT_TRUE(session->wait_success());
	client.join();

	int after = session->handle_calls;
	EXPECT_GT(after, 1); /* TOREPLY + terminal SUCCESS */
	Sleep(300);
	EXPECT_EQ(session->handle_calls, after);
	EXPECT_EQ(session->last_state, CS_STATE_SUCCESS);

	delete session;
	comm.unbind(&service);
	ASSERT_TRUE(service.wait_unbound());
	service.deinit();
	comm.deinit();
}

TEST(communicator, multiple_handler_threads_client_request_single_terminal)
{
	LoopbackListener listener;
	ASSERT_TRUE(listener.start());

	Communicator comm;
	ASSERT_EQ(comm.init(2, 2), 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(listener.port);

	TestTarget target;
	ASSERT_EQ(target.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);

	TestSession session(&comm);
	ASSERT_EQ(comm.request(&session, &target), 0);
	ASSERT_TRUE(session.wait_result());

	EXPECT_EQ(session.result_state, CS_STATE_SUCCESS);
	EXPECT_EQ(session.result_error, 0);
	EXPECT_EQ(session.handle_calls, 1);
	Sleep(200);
	EXPECT_EQ(session.handle_calls, 1);

	comm.deinit();
	target.deinit();
	listener.thread.join();
	listener.started = false;
}

/* ------------------------------------------------------------------------
 * Invalid-session-lifetime contract probes (currently disabled because
 * they crash).
 *
 * These tests intentionally delete a CommSession before the kernel has
 * delivered its terminal handle().  Under the Linux/Windows shared
 * CommSession contract this is an illegal lifetime usage: the session
 * must stay alive until terminal handle() returns.  They are NOT
 * correctness requirements for the Windows backend alone; enabling them
 * is expected to crash unless the public session ownership model is
 * changed in Linux and Windows together.
 *
 * They remain in the tree to document the boundary and to catch accidental
 * changes that would make even the legal contract unsafe.
 * ------------------------------------------------------------------------ */

TEST(communicator, DISABLED_invalid_session_lifetime_delete_server_session_after_reply)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	TestServerService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);
	ASSERT_EQ(comm.bind(&service), 0);

	std::thread client(ServerClient::run, port);
	TestServerSession *session = service.wait_accepted();
	ASSERT_TRUE(session != nullptr);
	ASSERT_TRUE(session->wait_toreply());

	/* reply() returns before the terminal write completion/event has been
	 * consumed. Deleting the session now leaves conn->session and the
	 * queued terminal io_result.comm_session dangling. */
	ASSERT_EQ(comm.reply(session), 0);
	delete session;

	client.join();
	Sleep(300);

	comm.unbind(&service);
	ASSERT_TRUE(service.wait_unbound());
	service.deinit();
	comm.deinit();
}

TEST(communicator, DISABLED_invalid_session_lifetime_delete_server_session_after_reply_keepalive)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	unsigned short port = pick_free_port();
	ASSERT_NE(port, 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);

	TestServerService service(&comm);
	ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);
	ASSERT_EQ(comm.bind(&service), 0);

	std::thread client(ServerClient::run, port);
	TestServerSession *session = service.wait_accepted();
	ASSERT_TRUE(session != nullptr);
	ASSERT_TRUE(session->wait_toreply());

	/* Keepalive path also queues a terminal io_result with a raw
	 * CommSession* after reply() returns.  Deleting the session before
	 * that event is consumed leaves the same dangling pointer. */
	session->keep_alive_ms = 5000;
	ASSERT_EQ(comm.reply(session), 0);
	delete session;

	client.join();
	Sleep(300);

	comm.unbind(&service);
	ASSERT_TRUE(service.wait_unbound());
	service.deinit();
	comm.deinit();
}

TEST(communicator, DISABLED_invalid_session_lifetime_delete_client_session_after_request)
{
	LoopbackListener listener;
	ASSERT_TRUE(listener.start());

	Communicator comm;
	ASSERT_EQ(comm.init(1, 1), 0);

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(listener.port);

	TestTarget target;
	ASSERT_EQ(target.init((const struct sockaddr *)&sin, sizeof sin,
						  1000, 1000), 0);

	TestSession *session = new TestSession(&comm);
	ASSERT_EQ(comm.request(session, &target), 0);

	/* request() returns before connect/response completes. Deleting the
	 * client session now leaves the pending terminal result with a raw
	 * CommSession* that is already gone. */
	delete session;

	Sleep(300);
	comm.deinit();
	target.deinit();
	listener.thread.join();
	listener.started = false;
}

/* ------------------------------------------------------------------------
 * Disabled race/stress probes.  These exercise interleavings that are
 * believed to be under-covered by the normal single-shot tests.  They are
 * kept disabled because some of them are expected to crash/fail under the
 * current raw-session architecture or because they are timing-sensitive.
 * Run them manually with --gtest_also_run_disabled_tests.
 * ------------------------------------------------------------------------ */

TEST(communicator, DISABLED_repeat_unbind_while_udp_reply_waiting_stress)
{
	for (int i = 0; i < 20; i++)
	{
		Communicator comm;
		ASSERT_EQ(comm.init(1, 1), 0);

		unsigned short port = pick_free_port();
		ASSERT_NE(port, 0);

		struct sockaddr_in sin;
		memset(&sin, 0, sizeof sin);
		sin.sin_family = AF_INET;
		sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		sin.sin_port = htons(port);

		TestDatagramService service(&comm);
		ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
							  1000, 1000), 0);
		service.set_reliable(0);
		ASSERT_EQ(comm.bind(&service), 0);

		std::thread sender(send_datagram, port, "hello", 5);
		TestDatagramSession *session = service.wait_accepted();
		ASSERT_TRUE(session != nullptr);
		ASSERT_TRUE(session->wait_toreply());

		std::atomic<int> reply_ret(-999);
		std::thread reply_thread([&] {
			reply_ret.store(comm.reply(session));
		});

		Sleep(1);
		comm.unbind(&service);
		reply_thread.join();

		EXPECT_TRUE(reply_ret.load() == 0 || reply_ret.load() == -1);
		EXPECT_TRUE(service.wait_unbound());

		delete session;
		sender.join();
		service.deinit();
		comm.deinit();
	}
}

TEST(communicator, DISABLED_repeat_stream_reply_and_client_close_stress)
{
	for (int i = 0; i < 20; i++)
	{
		Communicator comm;
		ASSERT_EQ(comm.init(1, 1), 0);

		unsigned short port = pick_free_port();
		ASSERT_NE(port, 0);

		struct sockaddr_in sin;
		memset(&sin, 0, sizeof sin);
		sin.sin_family = AF_INET;
		sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		sin.sin_port = htons(port);

		TestServerService service(&comm);
		ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
							  1000, 1000), 0);
		ASSERT_EQ(comm.bind(&service), 0);

		std::thread client(send_and_close_client, port);
		TestServerSession *session = service.wait_accepted();
		ASSERT_TRUE(session != nullptr);
		ASSERT_TRUE(session->wait_toreply());

		ASSERT_EQ(comm.reply(session), 0);
		ASSERT_TRUE(session->wait_success());
		client.join();

		int after = session->handle_calls;
		EXPECT_GT(after, 1);
		Sleep(20);
		EXPECT_EQ(session->handle_calls, after);

		delete session;
		comm.unbind(&service);
		ASSERT_TRUE(service.wait_unbound());
		service.deinit();
		comm.deinit();
	}
}

TEST(communicator, DISABLED_repeat_keepalive_timeout_and_client_close_stress)
{
	for (int i = 0; i < 10; i++)
	{
		Communicator comm;
		ASSERT_EQ(comm.init(1, 1), 0);

		unsigned short port = pick_free_port();
		ASSERT_NE(port, 0);

		struct sockaddr_in sin;
		memset(&sin, 0, sizeof sin);
		sin.sin_family = AF_INET;
		sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		sin.sin_port = htons(port);

		TestServerService service(&comm);
		ASSERT_EQ(service.init((const struct sockaddr *)&sin, sizeof sin,
							  1000, 1000), 0);
		ASSERT_EQ(comm.bind(&service), 0);

		std::thread client(ServerClient::run, port);
		TestServerSession *session = service.wait_accepted();
		ASSERT_TRUE(session != nullptr);
		ASSERT_TRUE(session->wait_toreply());

		session->keep_alive_ms = 50;
		ASSERT_EQ(comm.reply(session), 0);
		ASSERT_TRUE(session->wait_success());
		client.join();

		int after = session->handle_calls;
		EXPECT_GT(after, 1);
		Sleep(120);
		EXPECT_EQ(session->handle_calls, after);

		delete session;
		comm.unbind(&service);
		ASSERT_TRUE(service.wait_unbound());
		service.deinit();
		comm.deinit();
	}
}

} /* anonymous namespace */
