#include "Communicator.h"
#include "IOService.h"

#include <gtest/gtest.h>

#include <WinSock2.h>
#include <WS2tcpip.h>
#include <Windows.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <set>
#include <thread>

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

struct TestMessageOut : CommMessageOut
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

struct TestMessageIn : CommMessageIn
{
	/* The read filter completes when `need_bytes` bytes have arrived; with
	 * renew_on_append the first append renews the read deadline (the
	 * read_message_ctx renew flag), proving the per-request deadline can be
	 * reset mid-read. */
	int need_bytes = 1;
	int seen = 0;
	bool renew_on_append = false;

	int append(const void *buf, size_t *size) override
	{
		(void)buf;
		if (renew_on_append)
			renew();
		seen += (int)*size;
		return seen >= need_bytes ? 1 : 0;
	}
};

struct TestSession : CommSession
{
	TestMessageOut out;
	TestMessageIn in;
	std::mutex mutex;
	std::condition_variable cv;
	bool done = false;
	int state = -1;
	int error = -1;
	int keep_alive_ms = 0;
	int first_ms = 0;
	/* -1 matches the CommSession base default: no per-segment deadline. */
	int recv_ms = -1;

	TestSession() = default;

	CommMessageOut *message_out() override { return &out; }
	CommMessageIn *message_in() override { return &in; }
	int keep_alive_timeout() override { return keep_alive_ms; }
	int first_timeout() override { return first_ms; }
	int receive_timeout() override { return recv_ms; }

	void handle(int st, int err) override
	{
		std::lock_guard<std::mutex> lock(mutex);
		state = st;
		error = err;
		done = true;
		cv.notify_all();
	}

	bool wait(int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		return cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
						   [this] { return done; });
	}
};

struct TestTarget : CommTarget
{
	TestTarget()
	{
		std::memset(&addr_, 0, sizeof addr_);
	}

	bool init_loopback(unsigned short port, int connect_ms = 1000,
					   int response_ms = 1000)
	{
		addr_.sin_family = AF_INET;
		addr_.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		addr_.sin_port = htons(port);
		return CommTarget::init((const struct sockaddr *)&addr_, sizeof addr_,
								connect_ms, response_ms) == 0;
	}

	void set_udp()
	{
		set_transport(COMM_TRANSPORT_UDP);
	}

	void enable_ssl(SSL_CTX *ctx)
	{
		set_ssl(ctx, 1000);
	}

private:
	struct sockaddr_in addr_;
};

struct ServerThread
{
	SOCKET listen_sock = INVALID_SOCKET;
	unsigned short port = 0;
	std::thread thread;

	bool start()
	{
		listen_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
		if (listen_sock == INVALID_SOCKET)
			return false;

		struct sockaddr_in sin;
		std::memset(&sin, 0, sizeof sin);
		sin.sin_family = AF_INET;
		sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		sin.sin_port = 0;
		if (bind(listen_sock, (struct sockaddr *)&sin, sizeof sin) != 0 ||
			listen(listen_sock, 4) != 0)
		{
			closesocket(listen_sock);
			return false;
		}

		int len = sizeof sin;
		if (getsockname(listen_sock, (struct sockaddr *)&sin, &len) != 0)
		{
			closesocket(listen_sock);
			return false;
		}

		port = ntohs(sin.sin_port);
		thread = std::thread([this] { run(); });
		return true;
	}

	void run()
	{
		SOCKET client = accept(listen_sock, nullptr, nullptr);
		if (client == INVALID_SOCKET)
			return;

		char buf[16] = {};
		int n = recv(client, buf, sizeof buf, 0);
		if (n > 0)
			send(client, "world", 5, 0);

		closesocket(client);
		closesocket(listen_sock);
		listen_sock = INVALID_SOCKET;
	}

	void join()
	{
		if (thread.joinable())
			thread.join();
	}
};

struct TestSleepSession : public SleepSession
{
	std::mutex mutex;
	std::condition_variable cv;
	bool done = false;
	long long ms;
	int state = -1;
	int error = -1;

	explicit TestSleepSession(long long value_ms) : ms(value_ms)
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
		std::lock_guard<std::mutex> lock(mutex);
		this->state = state;
		this->error = error;
		done = true;
		cv.notify_all();
	}

	bool wait(int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		return cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
						   [this] { return done; });
	}
};

struct TestIOService : public IOService
{
	std::atomic<bool> unbound{false};

private:
	void handle_unbound() override
	{
		unbound = true;
	}
};

struct TestFsyncSession : public IOSession
{
	HANDLE file;
	std::mutex mutex;
	std::condition_variable cv;
	bool done = false;
	int state = -1;
	int error = -1;

	explicit TestFsyncSession(HANDLE h) : file(h) { }

	int prepare() override
	{
		prep_fsync(file);
		return 0;
	}

	void handle(int st, int err) override
	{
		std::lock_guard<std::mutex> lock(mutex);
		state = st;
		error = err;
		done = true;
		cv.notify_all();
	}

	bool wait()
	{
		std::unique_lock<std::mutex> lock(mutex);
		return cv.wait_for(lock, std::chrono::seconds(5),
						   [this] { return done; });
	}
};

struct TestFileIOSession : public IOSession
{
	enum Operation
	{
		READ,
		WRITE,
		READV,
		WRITEV
	};

	HANDLE file;
	Operation operation;
	void *buffer;
	const struct iovec *vectors;
	int vector_count;
	size_t count;
	int64_t offset;
	std::mutex mutex;
	std::condition_variable cv;
	bool done = false;
	int state = -1;
	int error = -1;
	long long result = -1;

	TestFileIOSession(HANDLE h, Operation op, void *buf, size_t n,
					 int64_t off)
		: file(h), operation(op), buffer(buf), vectors(nullptr),
			  vector_count(0), count(n), offset(off)
	{
	}

	TestFileIOSession(HANDLE h, Operation op, const struct iovec *iov,
					 int iovcnt, int64_t off)
		: file(h), operation(op), buffer(nullptr), vectors(iov),
			  vector_count(iovcnt), count(0), offset(off)
	{
		for (int i = 0; i < iovcnt; ++i)
			count += iov[i].iov_len;
	}

	int prepare() override
	{
		switch (operation)
		{
		case READ:
			prep_pread(file, buffer, count, offset);
			break;
		case WRITE:
			prep_pwrite(file, buffer, count, offset);
			break;
		case READV:
			prep_preadv(file, vectors, vector_count, offset);
			break;
		case WRITEV:
			prep_pwritev(file, vectors, vector_count, offset);
			break;
		default:
			errno = EINVAL;
			return -1;
		}
		return 0;
	}

	void handle(int st, int err) override
	{
		std::lock_guard<std::mutex> lock(mutex);
		state = st;
		error = err;
		result = get_res();
		done = true;
		cv.notify_all();
	}

	bool wait()
	{
		std::unique_lock<std::mutex> lock(mutex);
		return cv.wait_for(lock, std::chrono::seconds(5),
			[this] { return done; });
	}
};

struct TestServerSession : CommSession
{
	Communicator *comm;
	TestMessageOut out;
	TestMessageIn in;
	std::mutex mutex;
	std::condition_variable cv;
	bool done = false;
	int state = -1;
	int error = -1;

	explicit TestServerSession(Communicator *c) : comm(c) {}

	CommMessageOut *message_out() override { return &out; }
	CommMessageIn *message_in() override { return &in; }

	void handle(int st, int err) override
	{
		if (st == CS_STATE_TOREPLY)
		{
			comm->reply(this);
			return;
		}

		delete this;
	}

	bool wait(int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		return cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
						   [this] { return done; });
	}
};

struct TestService : CommService
{
	Communicator *comm;
	std::atomic<bool> unbound{false};
	std::atomic<bool> connection_seen{false};

	explicit TestService(Communicator *c) : comm(c) {}

	CommSession *new_session(long long, CommConnection *conn) override
	{
		connection_seen.store(conn != nullptr);
		return new TestServerSession(comm);
	}

	void enable_ssl(SSL_CTX *ctx)
	{
		set_ssl(ctx, 1000);
	}

	void handle_unbound() override { unbound.store(true); }
};

struct TrackingServerSession : CommSession
{
	Communicator *comm;
	TestMessageOut out;
	TestMessageIn in;
	std::mutex mutex;
	std::condition_variable cv;
	int callbacks = 0;
	int final_state = -1;
	int first_result = -2;
	int second_result = -2;
	int second_error = 0;
	bool target_seen = false;
	bool shutdown_only;

	TrackingServerSession(Communicator *c, bool shutdown)
		: comm(c), shutdown_only(shutdown)
	{
	}

	CommMessageOut *message_out() override { return &out; }
	CommMessageIn *message_in() override { return &in; }

	void handle(int state, int) override
	{
		std::lock_guard<std::mutex> lock(mutex);
		callbacks++;
		if (state == CS_STATE_TOREPLY)
		{
			target_seen = get_target() != nullptr;
			if (shutdown_only)
				first_result = comm->shutdown(this);
			else
			{
				first_result = comm->reply(this);
				second_result = comm->reply(this);
				second_error = errno;
			}
		}
		else
			final_state = state;
		cv.notify_all();
	}

	bool wait_callbacks(int count, int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(mutex);
		return cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
			[this, count] { return callbacks >= count; });
	}
};

struct TrackingService : CommService
{
	Communicator *comm;
	bool shutdown_only;
	std::atomic<TrackingServerSession *> session{nullptr};
	std::atomic<bool> unbound{false};

	TrackingService(Communicator *c, bool shutdown)
		: comm(c), shutdown_only(shutdown)
	{
	}

	CommSession *new_session(long long seq, CommConnection *) override
	{
		EXPECT_EQ(seq, 0);
		TrackingServerSession *s =
			new TrackingServerSession(comm, shutdown_only);
		session.store(s);
		return s;
	}

	void handle_unbound() override { unbound.store(true); }
};

/* Accepts one connection, reads one request, then stays silent for `hold_ms`
 * (first-timeout / deinit tests need a peer that never replies). */
struct SilentServerThread
{
	SOCKET listen_sock = INVALID_SOCKET;
	unsigned short port = 0;
	int hold_ms = 800;
	std::thread thread;

	bool start()
	{
		listen_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
		if (listen_sock == INVALID_SOCKET)
			return false;

		struct sockaddr_in sin;
		std::memset(&sin, 0, sizeof sin);
		sin.sin_family = AF_INET;
		sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		sin.sin_port = 0;
		if (bind(listen_sock, (struct sockaddr *)&sin, sizeof sin) != 0 ||
			listen(listen_sock, 4) != 0)
		{
			closesocket(listen_sock);
			return false;
		}

		int len = sizeof sin;
		if (getsockname(listen_sock, (struct sockaddr *)&sin, &len) != 0)
		{
			closesocket(listen_sock);
			return false;
		}

		port = ntohs(sin.sin_port);
		thread = std::thread([this] { run(); });
		return true;
	}

	void run()
	{
		SOCKET client = accept(listen_sock, nullptr, nullptr);
		if (client == INVALID_SOCKET)
			return;

		char buf[16] = {};
		recv(client, buf, sizeof buf, 0);
		Sleep((DWORD)hold_ms);
		closesocket(client);
		closesocket(listen_sock);
		listen_sock = INVALID_SOCKET;
	}

	void join()
	{
		if (thread.joinable())
			thread.join();
	}
};

/* Accepts exactly one connection and echoes two requests on it (client
 * keep-alive reuse: the second request must ride the same connection). */
struct EchoTwiceServerThread
{
	SOCKET listen_sock = INVALID_SOCKET;
	unsigned short port = 0;
	std::thread thread;
	std::atomic<int> accepts{0};
	std::atomic<int> echoes{0};

	bool start()
	{
		listen_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
		if (listen_sock == INVALID_SOCKET)
			return false;

		struct sockaddr_in sin;
		std::memset(&sin, 0, sizeof sin);
		sin.sin_family = AF_INET;
		sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		sin.sin_port = 0;
		if (bind(listen_sock, (struct sockaddr *)&sin, sizeof sin) != 0 ||
			listen(listen_sock, 4) != 0)
		{
			closesocket(listen_sock);
			return false;
		}

		int len = sizeof sin;
		if (getsockname(listen_sock, (struct sockaddr *)&sin, &len) != 0)
		{
			closesocket(listen_sock);
			return false;
		}

		port = ntohs(sin.sin_port);
		thread = std::thread([this] { run(); });
		return true;
	}

	void run()
	{
		SOCKET client = accept(listen_sock, nullptr, nullptr);
		if (client == INVALID_SOCKET)
			return;
		accepts.store(1);
		for (int i = 0; i < 2; ++i)
		{
			char buf[16] = {};
			int n = recv(client, buf, sizeof buf, 0);
			if (n <= 0)
				break;
			send(client, "world", 5, 0);
			echoes.store(i + 1);
		}
		closesocket(client);
		closesocket(listen_sock);
		listen_sock = INVALID_SOCKET;
	}

	void join()
	{
		if (thread.joinable())
			thread.join();
	}
};

struct KeepaliveCounter
{
	std::atomic<int> toreplies{0};
	std::mutex mutex;
	std::condition_variable cv;
	std::set<CommConnection *> conns;
};

/* Server session that replies on TOREPLY and keeps the connection alive,
 * so two requests on one connection produce two sessions and one CommConnection. */
struct KeepaliveServerSession : CommSession
{
	Communicator *comm;
	KeepaliveCounter *counter;
	TestMessageOut out;
	TestMessageIn in;

	KeepaliveServerSession(Communicator *c, KeepaliveCounter *k)
		: comm(c), counter(k)
	{
	}

	CommMessageOut *message_out() override { return &out; }
	CommMessageIn *message_in() override { return &in; }
	int keep_alive_timeout() override { return 30000; }

	void handle(int st, int) override
	{
		if (st == CS_STATE_TOREPLY)
		{
			counter->toreplies.fetch_add(1);
			counter->cv.notify_all();
			comm->reply(this);
			return;
		}
		delete this;
	}
};

struct KeepaliveService : CommService
{
	Communicator *comm;
	KeepaliveCounter *counter;
	std::atomic<bool> unbound{false};

	KeepaliveService(Communicator *c, KeepaliveCounter *k)
		: comm(c), counter(k)
	{
	}

	CommSession *new_session(long long, CommConnection *conn) override
	{
		{
			std::lock_guard<std::mutex> lock(counter->mutex);
			counter->conns.insert(conn);
		}
		return new KeepaliveServerSession(comm, counter);
	}

	void handle_unbound() override { unbound.store(true); }

	bool wait_toreplies(int count, int timeout_ms = 5000)
	{
		std::unique_lock<std::mutex> lock(counter->mutex);
		return counter->cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
			[this, count] { return counter->toreplies.load() >= count; });
	}
};

/* server.crt/server.key live at the workflow-win repo root; the test exe runs
 * below test/build.cmake. */
static bool repo_root_cert_path(char *cert, size_t cert_size,
								char *key, size_t key_size)
{
	char path[MAX_PATH];
	DWORD n = ::GetModuleFileNameA(nullptr, path, MAX_PATH);
	if (n == 0 || n >= MAX_PATH)
		return false;
	char *slash = strrchr(path, '\\');
	if (slash)
		*slash = '\0';
	for (int i = 0; i < 2; ++i)
	{
		slash = strrchr(path, '\\');
		if (!slash)
			return false;
		*slash = '\0';
	}
	_snprintf(cert, cert_size, "%s\\server.crt", path);
	_snprintf(key, key_size, "%s\\server.key", path);
	return true;
}

} /* namespace */

TEST(WindowsCommunicator, ClientRequestEcho)
{
	ServerThread server;
	ASSERT_TRUE(server.start());

	Communicator comm;
	ASSERT_EQ(comm.init(2), 0);

	TestSession session;
	TestTarget target;
	ASSERT_TRUE(target.init_loopback(server.port));
	ASSERT_EQ(comm.request(&session, &target), 0);
	ASSERT_TRUE(session.wait());

	EXPECT_EQ(session.state, CS_STATE_SUCCESS);
	EXPECT_EQ(session.error, 0);

	comm.deinit();
	server.join();
}

TEST(WindowsCommunicator, ServerBindAcceptReply)
{
	SOCKET tmp = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(tmp != INVALID_SOCKET);
	struct sockaddr_in tmp_addr;
	memset(&tmp_addr, 0, sizeof tmp_addr);
	tmp_addr.sin_family = AF_INET;
	tmp_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	tmp_addr.sin_port = 0;
	ASSERT_EQ(0, bind(tmp, (struct sockaddr *)&tmp_addr, sizeof tmp_addr));
	int tmp_len = sizeof tmp_addr;
	ASSERT_EQ(0, getsockname(tmp, (struct sockaddr *)&tmp_addr, &tmp_len));
	unsigned short port = ntohs(tmp_addr.sin_port);
	closesocket(tmp);

	Communicator comm;
	ASSERT_EQ(comm.init(2), 0);

	TestService service(&comm);
	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);
	ASSERT_EQ(service.init((struct sockaddr *)&sin, sizeof sin, 1000, 1000), 0);
	ASSERT_EQ(comm.bind(&service), 0);

	std::thread client([&]() {
		SOCKET c = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
		ASSERT_TRUE(c != INVALID_SOCKET);
		struct sockaddr_in server_addr;
		memset(&server_addr, 0, sizeof server_addr);
		server_addr.sin_family = AF_INET;
		server_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		server_addr.sin_port = htons(port);
		ASSERT_EQ(0, connect(c, (struct sockaddr *)&server_addr,
							 sizeof server_addr));
		ASSERT_EQ(5, send(c, "hello", 5, 0));
		char buf[16] = {};
		ASSERT_EQ(5, recv(c, buf, 5, 0));
		ASSERT_EQ(0, memcmp(buf, "hello", 5));
		closesocket(c);
	});
	client.join();
	EXPECT_TRUE(service.connection_seen.load());

	comm.unbind(&service);
	comm.deinit();
}

TEST(WindowsCommunicator, UdpClientRequestEcho)
{
	SOCKET server_sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	ASSERT_TRUE(server_sock != INVALID_SOCKET);
	struct sockaddr_in server_addr;
	memset(&server_addr, 0, sizeof server_addr);
	server_addr.sin_family = AF_INET;
	server_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	server_addr.sin_port = 0;
	ASSERT_EQ(0, bind(server_sock, (struct sockaddr *)&server_addr,
					  sizeof server_addr));
	int addr_len = sizeof server_addr;
	ASSERT_EQ(0, getsockname(server_sock, (struct sockaddr *)&server_addr,
							 &addr_len));

	std::thread server([&]() {
		char buf[16] = {};
		struct sockaddr_in from;
		int fromlen = sizeof from;
		int n = recvfrom(server_sock, buf, sizeof buf, 0,
						 (struct sockaddr *)&from, &fromlen);
		if (n > 0)
			sendto(server_sock, buf, n, 0, (struct sockaddr *)&from, fromlen);
	});

	Communicator comm;
	ASSERT_EQ(comm.init(1), 0);

	TestSession session;
	TestTarget target;
	ASSERT_TRUE(target.init_loopback(ntohs(server_addr.sin_port)));
	target.set_udp();
	ASSERT_EQ(comm.request(&session, &target), 0);
	ASSERT_TRUE(session.wait());

	EXPECT_EQ(session.state, CS_STATE_SUCCESS);
	EXPECT_EQ(session.error, 0);

	comm.deinit();
	server.join();
	closesocket(server_sock);
}

TEST(WindowsCommunicator, UdpServerBindRecvReply)
{
	SOCKET tmp = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	ASSERT_TRUE(tmp != INVALID_SOCKET);
	struct sockaddr_in tmp_addr;
	memset(&tmp_addr, 0, sizeof tmp_addr);
	tmp_addr.sin_family = AF_INET;
	tmp_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	tmp_addr.sin_port = 0;
	ASSERT_EQ(0, bind(tmp, (struct sockaddr *)&tmp_addr, sizeof tmp_addr));
	int tmp_len = sizeof tmp_addr;
	ASSERT_EQ(0, getsockname(tmp, (struct sockaddr *)&tmp_addr, &tmp_len));
	unsigned short port = ntohs(tmp_addr.sin_port);
	closesocket(tmp);

	Communicator comm;
	ASSERT_EQ(comm.init(1), 0);

	TestService service(&comm);
	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = htons(port);
	ASSERT_EQ(service.init((struct sockaddr *)&sin, sizeof sin, 1000, 1000), 0);
	service.set_reliable(0);
	ASSERT_EQ(comm.bind(&service), 0);

	SOCKET client = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	ASSERT_TRUE(client != INVALID_SOCKET);
	struct sockaddr_in server_addr;
	memset(&server_addr, 0, sizeof server_addr);
	server_addr.sin_family = AF_INET;
	server_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	server_addr.sin_port = htons(port);
	ASSERT_EQ(5, sendto(client, "hello", 5, 0,
						(struct sockaddr *)&server_addr, sizeof server_addr));
	char buf[16] = {};
	struct sockaddr_in from;
	int fromlen = sizeof from;
	int n = recvfrom(client, buf, sizeof buf, 0,
					 (struct sockaddr *)&from, &fromlen);
	ASSERT_EQ(5, n);
	ASSERT_EQ(0, memcmp(buf, "hello", 5));
	closesocket(client);
	EXPECT_TRUE(service.connection_seen.load());

	comm.unbind(&service);
	comm.deinit();
}

TEST(WindowsCommunicator, FileFsyncCompletesThroughIOService)
{
	char directory[MAX_PATH];
	char path[MAX_PATH];
	ASSERT_NE(0u, ::GetTempPathA(MAX_PATH, directory));
	ASSERT_NE(0u, ::GetTempFileNameA(directory, "wff", 0, path));
	HANDLE file = ::CreateFileA(path, GENERIC_READ | GENERIC_WRITE,
		FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr, CREATE_ALWAYS,
		FILE_FLAG_OVERLAPPED, nullptr);
	ASSERT_NE(INVALID_HANDLE_VALUE, file);

	Communicator comm;
	TestIOService service;
	ASSERT_EQ(0, comm.init(1));
	ASSERT_EQ(0, service.init(4));
	ASSERT_EQ(0, comm.io_bind(&service));

	TestFsyncSession session(file);
	ASSERT_EQ(0, service.request(&session));
	ASSERT_TRUE(session.wait());
	EXPECT_EQ(IOS_STATE_SUCCESS, session.state);
	EXPECT_EQ(0, session.error);

	comm.io_unbind(&service);
	comm.deinit();
	service.deinit();
	::CloseHandle(file);
	::DeleteFileA(path);
}

TEST(WindowsCommunicator, FileReadWriteVectorOperationsThroughIOService)
{
	char directory[MAX_PATH];
	char path[MAX_PATH];
	ASSERT_NE(0u, ::GetTempPathA(MAX_PATH, directory));
	ASSERT_NE(0u, ::GetTempFileNameA(directory, "wfi", 0, path));

	const char initial[] = "abcdefghij";
	HANDLE seed = ::CreateFileA(path, GENERIC_READ | GENERIC_WRITE,
		FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr, CREATE_ALWAYS,
		0, nullptr);
	ASSERT_NE(INVALID_HANDLE_VALUE, seed);
	DWORD written = 0;
	ASSERT_TRUE(::WriteFile(seed, initial, sizeof initial - 1, &written,
		nullptr));
	ASSERT_EQ((DWORD)(sizeof initial - 1), written);
	::CloseHandle(seed);

	HANDLE file = ::CreateFileA(path, GENERIC_READ | GENERIC_WRITE,
		FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr, OPEN_EXISTING,
		FILE_FLAG_OVERLAPPED, nullptr);
	ASSERT_NE(INVALID_HANDLE_VALUE, file);

	Communicator comm;
	TestIOService service;
	ASSERT_EQ(0, comm.init(1));
	ASSERT_EQ(0, service.init(4));
	ASSERT_EQ(0, comm.io_bind(&service));

	char read_buf[4] = {};
	TestFileIOSession read(file, TestFileIOSession::READ, read_buf,
		3, 1);
	ASSERT_EQ(0, service.request(&read));
	ASSERT_TRUE(read.wait());
	EXPECT_EQ(IOS_STATE_SUCCESS, read.state);
	EXPECT_EQ(0, read.error);
	EXPECT_EQ(3, read.result);
	EXPECT_EQ(0, memcmp(read_buf, "bcd", 3));

	char write_buf[] = "XYZ";
	TestFileIOSession write(file, TestFileIOSession::WRITE, write_buf,
		3, 0);
	ASSERT_EQ(0, service.request(&write));
	ASSERT_TRUE(write.wait());
	EXPECT_EQ(IOS_STATE_SUCCESS, write.state);
	EXPECT_EQ(0, write.error);
	EXPECT_EQ(3, write.result);

	char read_first[3] = {};
	char read_second[4] = {};
	struct iovec read_iov[2] = {
		{ read_first, 2 },
		{ read_second, 3 }
	};
	TestFileIOSession readv(file, TestFileIOSession::READV, read_iov, 2, 0);
	ASSERT_EQ(0, service.request(&readv));
	ASSERT_TRUE(readv.wait());
	EXPECT_EQ(IOS_STATE_SUCCESS, readv.state);
	EXPECT_EQ(0, readv.error);
	EXPECT_EQ(5, readv.result);
	EXPECT_EQ(0, memcmp(read_first, "XY", 2));
	EXPECT_EQ(0, memcmp(read_second, "Zde", 3));

	char write_first[] = "12";
	char write_second[] = "345";
	struct iovec write_iov[2] = {
		{ write_first, 2 },
		{ write_second, 3 }
	};
	TestFileIOSession writev(file, TestFileIOSession::WRITEV, write_iov, 2, 5);
	ASSERT_EQ(0, service.request(&writev));
	ASSERT_TRUE(writev.wait());
	EXPECT_EQ(IOS_STATE_SUCCESS, writev.state);
	EXPECT_EQ(0, writev.error);
	EXPECT_EQ(5, writev.result);

	char final_buf[11] = {};
	TestFileIOSession final_read(file, TestFileIOSession::READ, final_buf,
		10, 0);
	ASSERT_EQ(0, service.request(&final_read));
	ASSERT_TRUE(final_read.wait());
	EXPECT_EQ(IOS_STATE_SUCCESS, final_read.state);
	EXPECT_EQ(0, final_read.error);
	EXPECT_EQ(10, final_read.result);
	EXPECT_EQ(0, memcmp(final_buf, "XYZde12345", 10));

	comm.io_unbind(&service);
	comm.deinit();
	service.deinit();
	::CloseHandle(file);
	::DeleteFileA(path);
}

TEST(WindowsCommunicator, SleepCompletes)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1), 0);

	TestSleepSession session(0);
	ASSERT_EQ(comm.sleep(&session), 0);
	ASSERT_TRUE(session.wait());

	EXPECT_EQ(session.state, SS_STATE_COMPLETE);
	EXPECT_EQ(session.error, 0);

	comm.deinit();
}

TEST(WindowsCommunicator, UnsleepCompletesWithECANCELED)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1), 0);

	TestSleepSession session(10000);
	ASSERT_EQ(comm.sleep(&session), 0);
	ASSERT_EQ(comm.unsleep(&session), 0);
	ASSERT_TRUE(session.wait());

	EXPECT_EQ(session.state, SS_STATE_ERROR);
	EXPECT_EQ(session.error, ECANCELED);

	comm.deinit();
}

TEST(WindowsCommunicator, UnsleepAfterCompletionReturnsENOENT)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1), 0);

	TestSleepSession session(0);
	ASSERT_EQ(comm.sleep(&session), 0);
	ASSERT_TRUE(session.wait());

	errno = 0;
	EXPECT_EQ(comm.unsleep(&session), -1);
	EXPECT_EQ(errno, ENOENT);

	comm.deinit();
}

TEST(WindowsCommunicator, DeinitDuringSleepDisrupts)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1), 0);

	TestSleepSession session(10000);
	ASSERT_EQ(comm.sleep(&session), 0);

	comm.deinit();
	ASSERT_TRUE(session.wait());

	EXPECT_EQ(session.state, SS_STATE_DISRUPTED);
}

TEST(WindowsCommunicator, TcpImmediateUnbindDrainsAccept)
{
	Communicator comm;
	ASSERT_EQ(0, comm.init(1));

	TestService service(&comm);
	struct sockaddr_in addr = {};
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = 0;
	ASSERT_EQ(0, service.init((struct sockaddr *)&addr, sizeof addr, 0, 0));
	ASSERT_EQ(0, comm.bind(&service));

	comm.unbind(&service);
	comm.deinit();
	EXPECT_TRUE(service.unbound.load());
}

TEST(WindowsCommunicator, UdpImmediateUnbindDrainsReceive)
{
	Communicator comm;
	ASSERT_EQ(0, comm.init(1));

	TestService service(&comm);
	struct sockaddr_in addr = {};
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = 0;
	ASSERT_EQ(0, service.init((struct sockaddr *)&addr, sizeof addr, 0, 0));
	service.set_reliable(0);
	ASSERT_EQ(0, comm.bind(&service));

	comm.unbind(&service);
	comm.deinit();
	EXPECT_TRUE(service.unbound.load());
}

TEST(WindowsCommunicator, DeinitUnbindsLiveTcpService)
{
	Communicator comm;
	ASSERT_EQ(0, comm.init(1));

	TestService service(&comm);
	struct sockaddr_in addr = {};
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = 0;
	ASSERT_EQ(0, service.init((struct sockaddr *)&addr, sizeof addr, 0, 0));
	ASSERT_EQ(0, comm.bind(&service));

	comm.deinit();
	EXPECT_TRUE(service.unbound.load());
}

TEST(WindowsCommunicator, DeinitUnbindsLiveUdpService)
{
	Communicator comm;
	ASSERT_EQ(0, comm.init(1));

	TestService service(&comm);
	struct sockaddr_in addr = {};
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = 0;
	ASSERT_EQ(0, service.init((struct sockaddr *)&addr, sizeof addr, 0, 0));
	service.set_reliable(0);
	ASSERT_EQ(0, comm.bind(&service));

	comm.deinit();
	EXPECT_TRUE(service.unbound.load());
}

TEST(WindowsCommunicator, PassiveReplyMatchesLinuxCallbackContract)
{
	SOCKET tmp = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(tmp != INVALID_SOCKET);
	struct sockaddr_in addr = {};
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	ASSERT_EQ(0, bind(tmp, (struct sockaddr *)&addr, sizeof addr));
	int addrlen = sizeof addr;
	ASSERT_EQ(0, getsockname(tmp, (struct sockaddr *)&addr, &addrlen));
	unsigned short port = ntohs(addr.sin_port);
	closesocket(tmp);

	Communicator comm;
	ASSERT_EQ(0, comm.init(1));
	TrackingService service(&comm, false);
	addr.sin_port = htons(port);
	ASSERT_EQ(0, service.init((struct sockaddr *)&addr, sizeof addr, 0, 1000));
	ASSERT_EQ(0, comm.bind(&service));

	SOCKET client = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(client != INVALID_SOCKET);
	ASSERT_EQ(0, connect(client, (struct sockaddr *)&addr, sizeof addr));
	ASSERT_EQ(5, send(client, "hello", 5, 0));
	char buf[8] = {};
	ASSERT_EQ(5, recv(client, buf, sizeof buf, 0));
	closesocket(client);

	TrackingServerSession *session = service.session.load();
	ASSERT_NE(nullptr, session);
	ASSERT_TRUE(session->wait_callbacks(2));
	EXPECT_TRUE(session->target_seen);
	EXPECT_EQ(0, session->first_result);
	EXPECT_EQ(-1, session->second_result);
	EXPECT_EQ(ENOENT, session->second_error);
	EXPECT_EQ(CS_STATE_SUCCESS, session->final_state);

	comm.unbind(&service);
	comm.deinit();
	EXPECT_FALSE(service.unbound.load());
	delete session;
	EXPECT_TRUE(service.unbound.load());
}

TEST(WindowsCommunicator, PassiveShutdownDoesNotInvokeCompletionCallback)
{
	SOCKET tmp = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(tmp != INVALID_SOCKET);
	struct sockaddr_in addr = {};
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	ASSERT_EQ(0, bind(tmp, (struct sockaddr *)&addr, sizeof addr));
	int addrlen = sizeof addr;
	ASSERT_EQ(0, getsockname(tmp, (struct sockaddr *)&addr, &addrlen));
	unsigned short port = ntohs(addr.sin_port);
	closesocket(tmp);

	Communicator comm;
	ASSERT_EQ(0, comm.init(1));
	TrackingService service(&comm, true);
	addr.sin_port = htons(port);
	ASSERT_EQ(0, service.init((struct sockaddr *)&addr, sizeof addr, 0, 1000));
	ASSERT_EQ(0, comm.bind(&service));

	SOCKET client = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(client != INVALID_SOCKET);
	ASSERT_EQ(0, connect(client, (struct sockaddr *)&addr, sizeof addr));
	ASSERT_EQ(5, send(client, "hello", 5, 0));
	char byte;
	EXPECT_LE(recv(client, &byte, 1, 0), 0);
	closesocket(client);

	TrackingServerSession *session = service.session.load();
	ASSERT_NE(nullptr, session);
	ASSERT_TRUE(session->wait_callbacks(1));
	EXPECT_TRUE(session->target_seen);
	EXPECT_EQ(0, session->first_result);
	EXPECT_EQ(1, session->callbacks);

	comm.unbind(&service);
	comm.deinit();
	EXPECT_EQ(1, session->callbacks);
	delete session;
	EXPECT_TRUE(service.unbound.load());
}

TEST(WindowsCommunicator, ClientKeepaliveReuse)
{
	EchoTwiceServerThread server;
	ASSERT_TRUE(server.start());

	Communicator comm;
	ASSERT_EQ(comm.init(2), 0);

	TestSession session1;
	session1.keep_alive_ms = 30000;
	TestTarget target;
	ASSERT_TRUE(target.init_loopback(server.port));
	ASSERT_EQ(comm.request(&session1, &target), 0);
	ASSERT_TRUE(session1.wait());
	EXPECT_EQ(session1.state, CS_STATE_SUCCESS);
	EXPECT_EQ(session1.error, 0);

	/* The second request must reuse the parked idle connection: the server
	 * accepts exactly once and echoes both requests on that one socket. */
	TestSession session2;
	session2.keep_alive_ms = 30000;
	ASSERT_EQ(comm.request(&session2, &target), 0);
	ASSERT_TRUE(session2.wait());
	EXPECT_EQ(session2.state, CS_STATE_SUCCESS);
	EXPECT_EQ(session2.error, 0);

	EXPECT_EQ(server.accepts.load(), 1);
	EXPECT_EQ(server.echoes.load(), 2);

	comm.deinit();
	server.join();
}

TEST(WindowsCommunicator, ServerKeepaliveSessionReuse)
{
	SOCKET tmp = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(tmp != INVALID_SOCKET);
	struct sockaddr_in addr = {};
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	ASSERT_EQ(0, bind(tmp, (struct sockaddr *)&addr, sizeof addr));
	int addrlen = sizeof addr;
	ASSERT_EQ(0, getsockname(tmp, (struct sockaddr *)&addr, &addrlen));
	unsigned short port = ntohs(addr.sin_port);
	closesocket(tmp);

	Communicator comm;
	ASSERT_EQ(0, comm.init(2));
	KeepaliveCounter counter;
	KeepaliveService service(&comm, &counter);
	addr.sin_port = htons(port);
	ASSERT_EQ(0, service.init((struct sockaddr *)&addr, sizeof addr, 1000, 1000));
	ASSERT_EQ(0, comm.bind(&service));

	/* One connection, two request/reply cycles: the second session must ride
	 * the same CommConnection (keep-alive), never a new accept. */
	SOCKET client = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(client != INVALID_SOCKET);
	ASSERT_EQ(0, connect(client, (struct sockaddr *)&addr, sizeof addr));
	ASSERT_EQ(5, send(client, "hello", 5, 0));
	char buf[16] = {};
	ASSERT_EQ(5, recv(client, buf, 5, 0));
	ASSERT_EQ(0, memcmp(buf, "hello", 5));
	ASSERT_EQ(5, send(client, "hello", 5, 0));
	ASSERT_EQ(5, recv(client, buf, 5, 0));
	ASSERT_EQ(0, memcmp(buf, "hello", 5));
	closesocket(client);

	ASSERT_TRUE(service.wait_toreplies(2));
	EXPECT_EQ(1u, counter.conns.size());
	EXPECT_EQ(2, counter.toreplies.load());

	comm.unbind(&service);
	comm.deinit();
}

TEST(WindowsCommunicator, ConnectFailureReportsError)
{
	Communicator comm;
	ASSERT_EQ(comm.init(1), 0);

	TestSession session;
	TestTarget target;
	struct sockaddr_in blackhole = {};
	blackhole.sin_family = AF_INET;
	blackhole.sin_addr.s_addr = inet_addr("192.0.2.1"); /* TEST-NET-1: no host */
	blackhole.sin_port = htons(80);
	ASSERT_EQ(target.init((const struct sockaddr *)&blackhole,
						  sizeof blackhole, 200, 1000), 0);
	ASSERT_EQ(comm.request(&session, &target), 0);
	ASSERT_TRUE(session.wait());

	EXPECT_EQ(session.state, CS_STATE_ERROR);
	/* TEST-NET-1 is not guaranteed to silently drop packets on Windows.
	 * Depending on the local route and firewall, ConnectEx may complete
	 * immediately with a native connection error instead of reaching the
	 * deadline.  The timeout path is covered by FirstTimeoutReportsETIMEDOUT;
	 * this test checks that an immediate connect failure is preserved. */
	EXPECT_TRUE(session.error == ETIMEDOUT ||
				session.error == EACCES ||
				session.error == ECONNREFUSED ||
				session.error == ENETUNREACH ||
				session.error == EHOSTUNREACH)
		<< "unexpected connect error: " << session.error;

	comm.deinit();
}

TEST(WindowsCommunicator, FirstTimeoutReportsETIMEDOUT)
{
	SilentServerThread server;
	ASSERT_TRUE(server.start());

	Communicator comm;
	ASSERT_EQ(comm.init(2), 0);

	TestSession session;
	session.first_ms = 300;
	TestTarget target;
	ASSERT_TRUE(target.init_loopback(server.port));
	ASSERT_EQ(comm.request(&session, &target), 0);
	ASSERT_TRUE(session.wait());

	EXPECT_EQ(session.state, CS_STATE_ERROR);
	EXPECT_EQ(session.error, ETIMEDOUT);

	comm.deinit();
	server.join();
}

TEST(WindowsCommunicator, FirstTimeoutOverridesResponseTimeout)
{
	SOCKET listen_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(listen_sock != INVALID_SOCKET);
	struct sockaddr_in addr = {};
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	ASSERT_EQ(0, bind(listen_sock, (struct sockaddr *)&addr, sizeof addr));
	int addrlen = sizeof addr;
	ASSERT_EQ(0, getsockname(listen_sock, (struct sockaddr *)&addr, &addrlen));
	unsigned short port = ntohs(addr.sin_port);
	ASSERT_EQ(0, listen(listen_sock, 4));

	std::thread server([&]() {
		SOCKET c = accept(listen_sock, nullptr, nullptr);
		if (c == INVALID_SOCKET)
			return;
		char buf[16] = {};
		recv(c, buf, sizeof buf, 0);
		Sleep(150);
		send(c, "x", 1, 0);
		Sleep(50);
		send(c, "x", 1, 0);
		closesocket(c);
		closesocket(listen_sock);
	});

	Communicator comm;
	ASSERT_EQ(0, comm.init(2));

	TestSession session;
	session.first_ms = 300;
	session.in.need_bytes = 2;
	TestTarget target;
	ASSERT_TRUE(target.init_loopback(port, 1000, 100));
	ASSERT_EQ(0, comm.request(&session, &target));
	ASSERT_TRUE(session.wait());

	EXPECT_EQ(session.state, CS_STATE_SUCCESS);
	EXPECT_EQ(session.error, 0);

	comm.deinit();
	server.join();
}

TEST(WindowsCommunicator, ReceiveTimeoutConsumedByFirstRead)
{
	SOCKET listen_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(listen_sock != INVALID_SOCKET);
	struct sockaddr_in addr = {};
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	ASSERT_EQ(0, bind(listen_sock, (struct sockaddr *)&addr, sizeof addr));
	int addrlen = sizeof addr;
	ASSERT_EQ(0, getsockname(listen_sock, (struct sockaddr *)&addr, &addrlen));
	unsigned short port = ntohs(addr.sin_port);
	ASSERT_EQ(0, listen(listen_sock, 4));

	std::thread server([&]() {
		SOCKET c = accept(listen_sock, nullptr, nullptr);
		if (c == INVALID_SOCKET)
			return;
		char buf[16] = {};
		recv(c, buf, sizeof buf, 0);
		send(c, "x", 1, 0);
		Sleep(180);
		send(c, "x", 1, 0);
		closesocket(c);
		closesocket(listen_sock);
	});

	Communicator comm;
	ASSERT_EQ(0, comm.init(2));

	TestSession session;
	session.recv_ms = 100;
	session.in.need_bytes = 2;
	TestTarget target;
	ASSERT_TRUE(target.init_loopback(port, 1000, 300));
	ASSERT_EQ(0, comm.request(&session, &target));
	ASSERT_TRUE(session.wait());

	EXPECT_EQ(session.state, CS_STATE_SUCCESS);
	EXPECT_EQ(session.error, 0);

	comm.deinit();
	server.join();
}

TEST(WindowsCommunicator, TotalTimeoutDeadline)
{
	SOCKET listen_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(listen_sock != INVALID_SOCKET);
	struct sockaddr_in addr = {};
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	ASSERT_EQ(0, bind(listen_sock, (struct sockaddr *)&addr, sizeof addr));
	int addrlen = sizeof addr;
	ASSERT_EQ(0, getsockname(listen_sock, (struct sockaddr *)&addr, &addrlen));
	unsigned short port = ntohs(addr.sin_port);
	ASSERT_EQ(0, listen(listen_sock, 4));

	/* Trickle two bytes (150ms apart) then stall.  The total receive deadline
	 * (300ms) is longer than the per-segment deadline (200ms), so the first
	 * read keeps the total deadline and the third read must end at 300ms. */
	std::thread server([&]() {
		SOCKET c = accept(listen_sock, nullptr, nullptr);
		if (c == INVALID_SOCKET)
			return;
		char buf[16] = {};
		recv(c, buf, sizeof buf, 0);
		send(c, "x", 1, 0);
		Sleep(150);
		send(c, "x", 1, 0);
		Sleep(1000);
		closesocket(c);
		closesocket(listen_sock);
	});

	Communicator comm;
	ASSERT_EQ(comm.init(2), 0);

	TestSession session;
	session.recv_ms = 300; /* total receive deadline */
	session.in.need_bytes = 3; /* never reached: the total deadline fires first */
	TestTarget target;
	ASSERT_TRUE(target.init_loopback(port, 1000, 200));
	ASSERT_EQ(comm.request(&session, &target), 0);
	ASSERT_TRUE(session.wait());

	EXPECT_EQ(session.state, CS_STATE_ERROR);
	EXPECT_EQ(session.error, ETIMEDOUT);

	comm.deinit();
	server.join();
}

TEST(WindowsCommunicator, ReceiveTimeoutWithUnlimitedResponseIsFirstReadOnly)
{
	SOCKET listen_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(listen_sock != INVALID_SOCKET);
	struct sockaddr_in addr = {};
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	ASSERT_EQ(0, bind(listen_sock, (struct sockaddr *)&addr, sizeof addr));
	int addrlen = sizeof addr;
	ASSERT_EQ(0, getsockname(listen_sock, (struct sockaddr *)&addr, &addrlen));
	unsigned short port = ntohs(addr.sin_port);
	ASSERT_EQ(0, listen(listen_sock, 4));

	/* With no response timeout, receive_timeout limits only the first read.
	 * After that read succeeds, Linux Workflow consumes the receive budget and
	 * leaves the following message reads without a deadline. */
	std::thread server([&]() {
		SOCKET c = accept(listen_sock, nullptr, nullptr);
		if (c == INVALID_SOCKET)
			return;
		char buf[16] = {};
		recv(c, buf, sizeof buf, 0);
		Sleep(50);
		send(c, "x", 1, 0);
		Sleep(250);
		send(c, "x", 1, 0);
		closesocket(c);
		closesocket(listen_sock);
	});

	Communicator comm;
	ASSERT_EQ(0, comm.init(2));

	TestSession session;
	session.recv_ms = 200;
	session.in.need_bytes = 2;
	TestTarget target;
	ASSERT_TRUE(target.init_loopback(port, 1000, -1));
	ASSERT_EQ(0, comm.request(&session, &target));
	ASSERT_TRUE(session.wait());

	EXPECT_EQ(session.state, CS_STATE_SUCCESS);
	EXPECT_EQ(session.error, 0);

	comm.deinit();
	server.join();
}

TEST(WindowsCommunicator, RenewResetsDeadline)
{
	SOCKET listen_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(listen_sock != INVALID_SOCKET);
	struct sockaddr_in addr = {};
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	ASSERT_EQ(0, bind(listen_sock, (struct sockaddr *)&addr, sizeof addr));
	int addrlen = sizeof addr;
	ASSERT_EQ(0, getsockname(listen_sock, (struct sockaddr *)&addr, &addrlen));
	unsigned short port = ntohs(addr.sin_port);
	ASSERT_EQ(0, listen(listen_sock, 4));

	/* Two bytes 600ms apart with a 300ms total receive deadline: only a renew
	 * during the first segment (which clears the total deadline) lets this
	 * succeed; the per-segment deadline (2s) never fires. */
	std::thread server([&]() {
		SOCKET c = accept(listen_sock, nullptr, nullptr);
		if (c == INVALID_SOCKET)
			return;
		char buf[16] = {};
		recv(c, buf, sizeof buf, 0);
		send(c, "x", 1, 0);
		Sleep(600);
		send(c, "x", 1, 0);
		Sleep(500);
		closesocket(c);
		closesocket(listen_sock);
	});

	Communicator comm;
	ASSERT_EQ(comm.init(2), 0);

	TestSession session;
	session.recv_ms = 300; /* total receive deadline: fires at 300ms without renew */
	session.in.need_bytes = 2;
	session.in.renew_on_append = true;
	TestTarget target;
	ASSERT_TRUE(target.init_loopback(port, 1000, 2000));
	ASSERT_EQ(comm.request(&session, &target), 0);
	ASSERT_TRUE(session.wait());

	EXPECT_EQ(session.state, CS_STATE_SUCCESS);
	EXPECT_EQ(session.error, 0);

	comm.deinit();
	server.join();
}

TEST(WindowsCommunicator, ListenTimeoutKeepsAccepting)
{
	SOCKET tmp = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(tmp != INVALID_SOCKET);
	struct sockaddr_in addr = {};
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	ASSERT_EQ(0, bind(tmp, (struct sockaddr *)&addr, sizeof addr));
	int addrlen = sizeof addr;
	ASSERT_EQ(0, getsockname(tmp, (struct sockaddr *)&addr, &addrlen));
	unsigned short port = ntohs(addr.sin_port);
	closesocket(tmp);

	Communicator comm;
	ASSERT_EQ(comm.init(1), 0);
	TestService service(&comm);
	addr.sin_port = htons(port);
	ASSERT_EQ(0, service.init((struct sockaddr *)&addr, sizeof addr, 200, 1000));
	ASSERT_EQ(0, comm.bind(&service));

	/* The accept deadline fires at least twice; the service must re-arm the
	 * accept loop instead of stopping, and still serve a client afterwards. */
	Sleep(600);

	SOCKET client = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(client != INVALID_SOCKET);
	ASSERT_EQ(0, connect(client, (struct sockaddr *)&addr, sizeof addr));
	ASSERT_EQ(5, send(client, "hello", 5, 0));
	char buf[16] = {};
	ASSERT_EQ(5, recv(client, buf, 5, 0));
	ASSERT_EQ(0, memcmp(buf, "hello", 5));
	closesocket(client);
	EXPECT_TRUE(service.connection_seen.load());

	comm.unbind(&service);
	comm.deinit();
}

TEST(WindowsCommunicator, SslClientServerEcho)
{
	char cert[MAX_PATH], key[MAX_PATH];
	ASSERT_TRUE(repo_root_cert_path(cert, sizeof cert, key, sizeof key));

	SSL_CTX *ctx = SSL_CTX_new(TLS_method());
	ASSERT_NE(nullptr, ctx);
	ASSERT_EQ(1, SSL_CTX_use_certificate_file(ctx, cert, SSL_FILETYPE_PEM));
	ASSERT_EQ(1, SSL_CTX_use_PrivateKey_file(ctx, key, SSL_FILETYPE_PEM));

	SOCKET tmp = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	ASSERT_TRUE(tmp != INVALID_SOCKET);
	struct sockaddr_in addr = {};
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	ASSERT_EQ(0, bind(tmp, (struct sockaddr *)&addr, sizeof addr));
	int addrlen = sizeof addr;
	ASSERT_EQ(0, getsockname(tmp, (struct sockaddr *)&addr, &addrlen));
	unsigned short port = ntohs(addr.sin_port);
	closesocket(tmp);

	Communicator comm;
	ASSERT_EQ(comm.init(2), 0);

	TestService service(&comm);
	addr.sin_port = htons(port);
	ASSERT_EQ(0, service.init((struct sockaddr *)&addr, sizeof addr, 1000, 1000));
	service.enable_ssl(ctx);
	ASSERT_EQ(0, comm.bind(&service));

	TestSession session;
	TestTarget target;
	ASSERT_TRUE(target.init_loopback(port));
	target.enable_ssl(ctx);
	ASSERT_EQ(comm.request(&session, &target), 0);
	ASSERT_TRUE(session.wait());

	EXPECT_EQ(session.state, CS_STATE_SUCCESS);
	EXPECT_EQ(session.error, 0);
	EXPECT_TRUE(service.connection_seen.load());

	comm.unbind(&service);
	comm.deinit();
	SSL_CTX_free(ctx);
}

TEST(WindowsCommunicator, FileBlockingFallbackFsync)
{
	char directory[MAX_PATH];
	char path[MAX_PATH];
	ASSERT_NE(0u, ::GetTempPathA(MAX_PATH, directory));
	ASSERT_NE(0u, ::GetTempFileNameA(directory, "wfb", 0, path));
	/* No FILE_FLAG_OVERLAPPED: the handle is blocking, so fsync must take the
	 * blocking-pool fallback (post_blocking), not the IOCP path. */
	HANDLE file = ::CreateFileA(path, GENERIC_READ | GENERIC_WRITE,
		FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr, CREATE_ALWAYS,
		0, nullptr);
	ASSERT_NE(INVALID_HANDLE_VALUE, file);

	Communicator comm;
	TestIOService service;
	ASSERT_EQ(0, comm.init(1));
	ASSERT_EQ(0, service.init(4));
	ASSERT_EQ(0, comm.io_bind(&service));

	TestFsyncSession session(file);
	ASSERT_EQ(0, service.request(&session));
	ASSERT_TRUE(session.wait());
	EXPECT_EQ(IOS_STATE_SUCCESS, session.state);
	EXPECT_EQ(0, session.error);

	comm.io_unbind(&service);
	comm.deinit();
	service.deinit();
	::CloseHandle(file);
	::DeleteFileA(path);
}

TEST(WindowsCommunicator, DeinitWithInflightRequestDrains)
{
	SilentServerThread server;
	ASSERT_TRUE(server.start());

	Communicator comm;
	ASSERT_EQ(comm.init(2), 0);

	TestSession session;
	TestTarget target;
	ASSERT_TRUE(target.init_loopback(server.port));
	ASSERT_EQ(comm.request(&session, &target), 0);

	/* Let connect/write submit and the read go in flight (the server never
	 * replies), then tear down: the in-flight request must drain with
	 * ECANCELED instead of hanging or crashing. */
	::Sleep(200);
	comm.deinit();
	ASSERT_TRUE(session.wait());
	EXPECT_EQ(session.state, CS_STATE_ERROR);
	EXPECT_EQ(session.error, ECANCELED);

	server.join();
}
