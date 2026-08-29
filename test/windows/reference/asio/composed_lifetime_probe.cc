/*
 * Probe: timed idle read handler/callback lifetime (M-A audit item #2).
 *
 * Reproduces the P1 UAF: a timed operation whose completion handler is
 * dispatched (queued on the strand) can have its destroy_context invoked
 * before the handler runs, when the losing child's release retires the last
 * reference.  The idle-read callback then observes a context that was already
 * released.
 *
 * The probe drives the real async/ stack over a loopback TCP pair:
 *   - arm a timed idle read on the server side
 *   - cancel it through the cancellation signal (as request_idle_entry does)
 *   - assert the callback runs exactly once with a live context, and the
 *     destroy hook runs exactly once, never before the callback
 *
 * Build (manual, like the other asio_reference probes):
 *   cl /nologo /EHsc /std:c++17 /MTd /Zi /I ..\..\src\kernel_win
 *       composed_lifetime_probe.cc ..\..\_lib\workflow.lib ws2_32.lib mswsock.lib
 */

#include "async/io_context.h"
#include "async/tcp_socket.h"
#include "async/tcp_acceptor.h"
#include "async/steady_timer.h"
#include "async/executor.h"
#include "async/op/composed_operations.h"

#include <WinSock2.h>
#include <Windows.h>

#include <cstdio>
#include <cstring>

struct probe_ctx
{
	volatile LONG callback_count;
	volatile LONG destroy_count;
	volatile LONG live;          /* released exactly once at destroy */
	volatile LONG callback_live; /* live as seen by the callback */
};

static void probe_cb(void *arg, int error, size_t bytes)
{
	probe_ctx *ctx = static_cast<probe_ctx *>(arg);
	::InterlockedIncrement(&ctx->callback_count);
	::InterlockedExchange(&ctx->callback_live,
		::InterlockedCompareExchange(&ctx->live, 0, 0));
	printf("  callback: error=%d bytes=%zu live=%d\n", error, bytes,
		   ::InterlockedCompareExchange(&ctx->live, 0, 0));
}

static void probe_destroy(void *arg)
{
	probe_ctx *ctx = static_cast<probe_ctx *>(arg);
	::InterlockedIncrement(&ctx->destroy_count);
	::InterlockedExchange(&ctx->live, 0);
	printf("  destroy: count=%d\n",
		   ::InterlockedCompareExchange(&ctx->destroy_count, 0, 0));
}

static int make_loopback_pair(tcp_socket *a, tcp_socket *b, io_context *io)
{
	int step = 0;
	tcp_acceptor acceptor;
	if (acceptor.init(executor(*io)) != 0)
		goto fail;
	step = 1;
	SOCKET listener = ::WSASocketW(AF_INET, SOCK_STREAM, IPPROTO_TCP,
		nullptr, 0, WSA_FLAG_OVERLAPPED);
	if (listener == INVALID_SOCKET)
		goto fail;
	step = 2;
	struct sockaddr_in sin;
	memset(&sin, 0, sizeof sin);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sin.sin_port = 0;
	if (::bind(listener, (struct sockaddr *)&sin, sizeof sin) != 0 ||
		::listen(listener, 4) != 0)
		goto fail;
	step = 3;
	int len = sizeof sin;
	if (::getsockname(listener, (struct sockaddr *)&sin, &len) != 0)
		goto fail;
	step = 4;
	if (acceptor.assign(listener) != 0)
		goto fail;
	step = 5;
	acceptor.listen(4);

	SOCKET sa = ::WSASocketW(AF_INET, SOCK_STREAM, IPPROTO_TCP,
		nullptr, 0, WSA_FLAG_OVERLAPPED);
	if (sa == INVALID_SOCKET)
		goto fail;
	step = 6;
	if (a->init(executor(*io)) != 0 || a->assign(sa) != 0)
		goto fail;
	step = 7;
	if (::connect(sa, (struct sockaddr *)&sin, sizeof sin) != 0 &&
		::WSAGetLastError() != WSAEWOULDBLOCK)
		goto fail;
	step = 8;

	SOCKET sb = ::accept(listener, nullptr, nullptr);
	if (sb == INVALID_SOCKET)
		goto fail;
	step = 9;
	if (b->init(executor(*io)) != 0 || b->assign(sb) != 0)
		goto fail;
	step = 10;
	acceptor.close();
	return 0;
fail:
	printf("setup failed: step=%d errno=%d wsa=%d\n", step, errno,
		   ::WSAGetLastError());
	return -1;
}

int main()
{
	WSADATA wsa;
	if (::WSAStartup(MAKEWORD(2, 2), &wsa) != 0)
		return 2;

	io_context io;
	if (io.init() != 0)
	{
		printf("io init failed\n");
		return 2;
	}
	executor ex(io);
	tcp_socket server;
	tcp_socket client;
	if (make_loopback_pair(&server, &client, &io) != 0)
	{
		printf("setup failed\n");
		return 2;
	}

	steady_timer timer;
	if (timer.init(ex) != 0)
	{
		printf("timer init failed\n");
		return 2;
	}

	probe_ctx ctx = {0, 0, 1, 0};
	cancellation_signal signal;

	printf("scenario: timed idle read, cancel through signal\n");
	int ret = timed_idle_read_start(&server, &timer, 5000,
		&probe_cb, &ctx, signal.slot(), &probe_destroy);
	if (ret != 0)
	{
		printf("  start failed: %d\n", ret);
		return 2;
	}

	signal.emit(cancellation_type::terminal);

	/* Drive the loop until the callback and destroy hooks have both run. */
	int spins = 0;
	while ((::InterlockedCompareExchange(&ctx.callback_count, 0, 0) == 0 ||
			::InterlockedCompareExchange(&ctx.destroy_count, 0, 0) == 0) &&
		   spins++ < 200)
		io.run_one();

	printf("  callback_count=%d destroy_count=%d\n",
		   ::InterlockedCompareExchange(&ctx.callback_count, 0, 0),
		   ::InterlockedCompareExchange(&ctx.destroy_count, 0, 0));

	int failures = 0;
	if (::InterlockedCompareExchange(&ctx.callback_count, 0, 0) != 1)
	{
		printf("FAIL: callback count != 1\n");
		++failures;
	}
	if (::InterlockedCompareExchange(&ctx.destroy_count, 0, 0) != 1)
	{
		printf("FAIL: destroy count != 1 (ctx released exactly once)\n");
		++failures;
	}
	if (::InterlockedCompareExchange(&ctx.callback_live, 0, 0) != 1)
	{
		/* destroy ran before the callback: the callback saw a released
		 * context — this is the P1 UAF signature. */
		printf("FAIL: destroy ran before callback (P1 UAF signature)\n");
		++failures;
	}

	server.close();
	client.close();
	io.shutdown();
	return failures ? 1 : 0;
}
