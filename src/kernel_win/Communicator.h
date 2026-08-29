/*
 * V6 Communicator upper-business contract.
 *
 * This header follows the Linux Workflow upper business shape, adapted only
 * where Windows requires SOCKET/HANDLE types. It intentionally exposes no
 * ASIO/IOCP/backend type. All lower execution is delegated to the V6 async kernel
 * (see async/).
 */

#ifndef _V6_COMMUNICATOR_H_
#define _V6_COMMUNICATOR_H_

#include <stddef.h>
#include <stdint.h>
#include <time.h>
#include <WinSock2.h>
#include <Windows.h>
#include <openssl/ssl.h>
#include "PlatformSocket.h"
#include "list.h"
#include "IOService.h"





class CommunicatorImpl;
class comm_request_op;
class comm_sleep_op;
class comm_service_op;

enum
{
	COMM_TRANSPORT_TCP = 1,
	COMM_TRANSPORT_UDP,
	COMM_TRANSPORT_SCTP
};

class Communicator;
class CommTarget;
class CommConnection;
class CommMessageOut;
class CommMessageIn;
class CommSession;
class CommService;
class CommServiceTarget;
class SleepSession;
class CommConnEntry;

class CommConnection
{
public:
	virtual ~CommConnection() { }
};

class CommTarget
{
public:
	CommTarget();
	int init(const struct sockaddr *addr, socklen_t addrlen,
			 int connect_timeout, int response_timeout);
	void deinit();

public:
	void get_addr(const struct sockaddr **addr, socklen_t *addrlen) const;
	int has_idle_conn() const;

protected:
	void set_ssl(SSL_CTX *ssl_ctx, int ssl_connect_timeout);
	SSL_CTX *get_ssl_ctx() const;
	virtual int transport() const;
	/* Kept for custom targets that configure their identity before request(). */
	void set_transport(int transport);

private:
	virtual SOCKET create_connect_socket();
	virtual CommConnection *new_connection(SOCKET socket);
	virtual int init_ssl(SSL *ssl);

public:
	virtual void release();
	virtual ~CommTarget();

private:
	struct sockaddr *addr;
	socklen_t addrlen;
	int connect_timeout;
	int response_timeout;
	int ssl_connect_timeout;
	SSL_CTX *ssl_ctx;
	/* Configuration for the default target implementation. */
	int transport_kind;

private:
	struct list_head idle_list;
	SRWLOCK lock;

	friend class Communicator;
	friend class CommunicatorImpl;
	friend class comm_request_op;
	friend class comm_sleep_op;
	friend class comm_service_op;
	friend class CommServiceTarget;
};

class CommMessageOut
{
private:
	virtual int encode(struct iovec vectors[], int max) = 0;

public:
	virtual ~CommMessageOut() { }

	friend class Communicator;
	friend class CommunicatorImpl;
	friend class comm_request_op;
	friend class comm_sleep_op;
	friend class comm_service_op;
};

class CommMessageIn
{
private:
	virtual int append(const void *buf, size_t *size) = 0;

protected:
	virtual int feedback(const void *buf, size_t size);
	virtual void renew();
	virtual CommMessageIn *inner();

private:
	CommConnEntry *entry;

public:
	CommMessageIn() : entry(NULL) { }
	virtual ~CommMessageIn() { }

	friend class Communicator;
	friend class CommunicatorImpl;
	friend class comm_request_op;
	friend class comm_sleep_op;
	friend class comm_service_op;
};

#define CS_STATE_SUCCESS	0
#define CS_STATE_ERROR		1
#define CS_STATE_STOPPED	2
#define CS_STATE_TOREPLY	3

class CommSession
{
private:
	virtual CommMessageOut *message_out() = 0;
	virtual CommMessageIn *message_in() = 0;
	virtual int send_timeout() { return -1; }
	virtual int receive_timeout() { return -1; }
	virtual int keep_alive_timeout() { return 0; }
	virtual int first_timeout() { return 0; }
	virtual void handle(int state, int error) = 0;

protected:
	CommTarget *get_target() const { return this->target; }
	CommConnection *get_connection() const { return this->conn; }
	CommMessageOut *get_message_out() const { return this->out; }
	CommMessageIn *get_message_in() const { return this->in; }
	long long get_seq() const { return this->seq; }

private:
	CommTarget *target;
	CommConnection *conn;
	CommMessageOut *out;
	CommMessageIn *in;
	long long seq;

private:
	int passive;

public:
	CommSession();
	virtual ~CommSession();

	friend class CommMessageIn;
	friend class Communicator;
	friend class CommunicatorImpl;
	friend class comm_request_op;
	friend class comm_sleep_op;
	friend class comm_service_op;
};

class CommService
{
public:
	int init(const struct sockaddr *bind_addr, socklen_t addrlen,
			 int listen_timeout, int response_timeout);
	void deinit();

	int drain(int max);

public:
	void get_addr(const struct sockaddr **addr, socklen_t *addrlen) const;
	void set_reliable(int reliable);

protected:
	void set_ssl(SSL_CTX *ssl_ctx, int ssl_accept_timeout);
	SSL_CTX *get_ssl_ctx() const;

private:
	virtual CommSession *new_session(long long seq, CommConnection *conn) = 0;
	virtual void handle_stop(int error);
	virtual void handle_unbound() = 0;

private:
	virtual SOCKET create_listen_socket();
	virtual SOCKET create_datagram_socket();
	virtual CommConnection *new_connection(SOCKET socket);
	virtual int init_ssl(SSL *ssl);

private:
	struct sockaddr *bind_addr;
	socklen_t addrlen;
	int listen_timeout;
	int response_timeout;
	int ssl_accept_timeout;
	SSL_CTX *ssl_ctx;

private:
	void incref();
	void decref();

private:
	int reliable;
	volatile LONG ref;
	CommunicatorImpl *impl;
	void *listener_handle;
	void *recv_handle;
	long long seq;
	volatile LONG closing;
	volatile LONG listener_released;
	struct list_head live_list;

private:
	struct list_head keep_alive_list;
	SRWLOCK lock;

public:
	virtual ~CommService();

	friend class Communicator;
	friend class CommServiceTarget;
	friend class CommunicatorImpl;
	friend class comm_request_op;
	friend class comm_sleep_op;
	friend class comm_service_op;
};

#define SS_STATE_COMPLETE	0
#define SS_STATE_ERROR		1
#define SS_STATE_DISRUPTED	2

class SleepSession
{
private:
	virtual int duration(struct timespec *value) = 0;
	virtual void handle(int state, int error) = 0;

private:
	volatile LONG state;
	void *timer_handle;

public:
	SleepSession() : state(0), timer_handle(nullptr) { }
	virtual ~SleepSession();

	friend class Communicator;
	friend class CommunicatorImpl;
	friend class comm_request_op;
	friend class comm_sleep_op;
	friend class comm_service_op;
};

class CommunicatorImpl;
class comm_request_op;
class comm_sleep_op;
class comm_service_op;

class CommEventHandler
{
private:
	virtual void schedule(void (*routine)(void *), void *context) = 0;
	virtual void wait() = 0;

public:
	virtual ~CommEventHandler() { }

	friend class Communicator;
	friend class CommunicatorImpl;
};

class Communicator
{
public:
	Communicator();
	virtual ~Communicator();

	int init(size_t io_threads);
	int init(size_t io_threads, size_t handler_threads);
	void deinit();

	int request(CommSession *session, CommTarget *target);
	int reply(CommSession *session);
	int push(const void *buf, size_t size, CommSession *session);
	int shutdown(CommSession *session);

	int bind(CommService *service);
	void unbind(CommService *service);

	int sleep(SleepSession *session);
	int unsleep(SleepSession *session);

	int io_bind(IOService *service);
	void io_unbind(IOService *service);

	/* Keep the Linux Workflow scheduler surface.  IO workers run the ASIO
	 * transport; handler workers run the final Workflow callbacks. */
	int is_handler_thread() const;
	int increase_handler_thread();
	int decrease_handler_thread();

	/* The handler is used only for final Workflow-visible completions.  It
	 * must remain alive until deinit() has returned. */
	void customize_event_handler(CommEventHandler *handler);

private:
	Communicator(const Communicator&);
	Communicator& operator=(const Communicator&);

	CommunicatorImpl *impl_;

	friend class IOService;
	int io_request(IOSession *session);
};

#endif /* _V6_COMMUNICATOR_H_ */







