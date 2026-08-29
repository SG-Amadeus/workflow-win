/*
 * AsyncCore: Communicator resource/connection objects.
 *
 * This layer owns Communicator transport resources and lifecycle bookkeeping.
 * It is business-layer code and must not be placed under async/.
 */

#ifndef _ASYNC_COMM_CONN_H_
#define _ASYNC_COMM_CONN_H_

#include "../Communicator.h"
#include "../async/async_kernel.h"
#include "../async/random_access_handle.h"
#include "../async/ssl_stream.h"
#include "../async/steady_timer.h"
#include "../async/strand.h"
#include "../async/tcp_acceptor.h"
#include "../async/tcp_socket.h"
#include "../async/udp_socket.h"
#include "../thrdpool.h"

#include <WinSock2.h>
#include <Windows.h>

#include <stddef.h>
#include <stdint.h>

#define V6_ENCODE_IOV_MAX 2048
#define V6_READ_BUFSIZE (64 * 1024)

enum
{
	CONN_STATE_CONNECTING = 0,
	CONN_STATE_CONNECTED,
	CONN_STATE_RECEIVING,
	CONN_STATE_SUCCESS,
	CONN_STATE_IDLE,
	CONN_STATE_KEEPALIVE,
	CONN_STATE_REUSING,
	CONN_STATE_CLOSING,
	CONN_STATE_ERROR
};

class CommConnEntry;
class comm_request_op;
class UdpServiceContext;
class FileIOContext;
class CommunicatorImpl;

class FileHandleContext
{
public:
	struct list_head list;
	HANDLE source;
	random_access_handle file;
	volatile LONG refs;
	DWORD volume_serial;
	DWORD file_index_high;
	DWORD file_index_low;
	bool identity_valid;
	bool cached;

	FileHandleContext(HANDLE h);
	int init(io_context *io);
};

class CommConnEntry
{
public:
	strand serial;
	/* Per-role deadline timers (ASIO server.cpp session owns input_deadline_/
	 * output_deadline_): request-flow phases (connect/handshake/write/udp send)
	 * arm output_timer.  Client pooled connections arm idle_timer only; server
	 * keep-alive timeout belongs to the server message-read operation.  A timer
	 * is never shared across concurrent roles, so one operation's cancellation
	 * cannot disturb another operation's pending wait. */
	steady_timer output_timer;
	steady_timer idle_timer;
	tcp_socket tcp_res;
	ssl_stream ssl_res;
	udp_socket udp_res;

	struct list_head list;
	struct list_head live_list;
	CommConnection *conn;
	long long seq;
	volatile LONG state;
	volatile LONG refs;
	volatile LONG owner_released;
	volatile LONG retired;
	comm_request_op *request_op;
	volatile LONG idle_timer_active;
	CommSession *session;
	CommTarget *target;
	CommService *service;
	CommunicatorImpl *impl;

	tcp_socket *tcp;
	ssl_stream *ssl_sock;
	udp_socket *udp_sock;
	bool udp_shared;
	UdpServiceContext *udp_context;
	struct sockaddr_storage udp_from;
	int udp_fromlen;

	CommConnEntry(CommunicatorImpl *i, CommSession *s, CommConnection *c,
				  CommTarget *t, CommService *svc = nullptr);
	~CommConnEntry();

	static CommConnEntry *create(CommunicatorImpl *i, io_context *io,
								 CommSession *s,
								 CommConnection *c, CommTarget *t,
								 CommService *svc = nullptr);
	int construct_tcp();
	int construct_udp();
	int construct_ssl(SSL_CTX *ctx, int server);
	void destroy_transport();
	void close_transport();
	void destroy_connection();
	void release_owner();
	void release();
};

class CommServiceTarget : public CommTarget
{
public:
	static CommServiceTarget *create(CommService *service,
									 const struct sockaddr *addr,
									 socklen_t addrlen);
	void incref();
	void release() override;

private:
	explicit CommServiceTarget(CommService *service);
	~CommServiceTarget() override;

	volatile LONG ref_;
	CommService *service_;
};

class UdpServiceContext
{
public:
	CommService *service;
	strand serial;
	udp_socket socket;
	udp_socket *sock;
	volatile LONG refs;
	char buf[V6_READ_BUFSIZE];
	struct sockaddr_storage from;
	int fromlen;

	explicit UdpServiceContext(CommService *svc);
	int init(io_context *io);
	void acquire();
	void release();
};

class TcpServiceContext
{
public:
	strand serial;
	steady_timer accept_timer;
	tcp_acceptor acceptor;
	volatile LONG refs;

	TcpServiceContext();
	int init(io_context *io);
	void acquire();
	void release();
};

class FileIOContext
{
public:
	struct list_head live_list;
	CommunicatorImpl *impl;
	IOSession *session;
	FileHandleContext *handle;

	FileIOContext(CommunicatorImpl *i, IOSession *s,
				  FileHandleContext *h);
};

class CommunicatorImpl
{
public:
	CommunicatorImpl();
	~CommunicatorImpl();

	async_kernel kernel;
	bool started = false;
	SRWLOCK lifecycle_lock;
	struct list_head live_entries;
	struct list_head live_services;
	struct list_head live_io_services;
	struct list_head live_files;
	struct list_head live_sleeps;
	struct list_head file_handles;
	volatile LONG shutting_down;
	SRWLOCK sleep_lock;
	thrdpool_t *handler_pool;
	CommEventHandler *event_handler;

	bool register_entry(CommConnEntry *entry);
	bool register_service(CommService *service);
	int bind_io_service(Communicator *owner, IOService *service);
	void unbind_io_service(IOService *service);
	void unbind_io_services();
	bool register_file(FileIOContext *context);
	void unregister_entry(CommConnEntry *entry);
	void unregister_service(CommService *service);
	void unregister_file(FileIOContext *context);
	FileHandleContext *acquire_file_handle(HANDLE handle);
	void release_file_handle(FileHandleContext *context);
	void destroy_file_handles();
	int post_handler(void (*routine)(void *), void *context);
	int post_handler(void (*routine)(void *), void *context,
					struct thrdpool_task_entry *storage);
	int init_handler_pool(size_t threads);
	void destroy_handler_pool();
	void shutdown();

private:
	static void run_pending_handler(const struct thrdpool_task *task);
};

#endif /* _ASYNC_COMM_CONN_H_ */
