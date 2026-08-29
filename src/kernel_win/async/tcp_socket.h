/*
  AsyncCore: minimal non-template TCP socket, named after ASIO.

  This is the simplified equivalent of asio::ip::tcp::socket.
  Connect is exclusive. One read and one write operation may be outstanding
  concurrently, matching ASIO's stream-socket concurrency contract.

  Completion callbacks are posted through the associated executor, so a
  strand-backed socket keeps handlers serialized.
*/

#ifndef _ASYNC_TCP_SOCKET_H_
#define _ASYNC_TCP_SOCKET_H_

#include "executor.h"
#include "op/cancellation.h"

#include <WinSock2.h>

#include <stddef.h>

#include "../../PlatformSocket.h"

class tcp_socket
{
public:
	tcp_socket();
	~tcp_socket();
	static tcp_socket *create(executor ex);
	static void destroy(tcp_socket *socket);
	int init(executor ex);

	tcp_socket(const tcp_socket &) = delete;
	tcp_socket &operator=(const tcp_socket &) = delete;
	int open();
	int open(int family);
	int assign(SOCKET socket);
	int close();

	int async_connect(const struct sockaddr *addr, int addrlen,
					  void (*callback)(void *, async_error_code), void *context);
	int async_connect(const struct sockaddr *addr, int addrlen,
					  void (*callback)(void *, async_error_code), void *context,
					  void (*destroy)(void *));
	int async_connect(const struct sockaddr *addr, int addrlen,
					  void (*callback)(void *, async_error_code), void *context,
					  const cancellation_slot &slot, void (*destroy)(void *));
	int async_read_some(void *buf, size_t size,
						void (*callback)(void *, async_error_code, size_t),
						void *context);
	int async_read_some(void *buf, size_t size,
						void (*callback)(void *, async_error_code, size_t),
						void *context, void (*destroy)(void *));
	int async_read_some(void *buf, size_t size,
						void (*callback)(void *, async_error_code, size_t), void *context,
						const cancellation_slot &slot, void (*destroy)(void *));
	int async_readv_some(const struct iovec *iov, int iovcnt,
						 void (*callback)(void *, async_error_code, size_t),
						 void *context);
	int async_readv_some(const struct iovec *iov, int iovcnt,
						 void (*callback)(void *, async_error_code, size_t),
						 void *context, void (*destroy)(void *));
	int async_readv_some(const struct iovec *iov, int iovcnt,
						 void (*callback)(void *, async_error_code, size_t), void *context,
						 const cancellation_slot &slot, void (*destroy)(void *));
	int async_wait_read(void (*callback)(void *, async_error_code, size_t),
						void *context, void (*destroy)(void *) = nullptr);
	int async_wait_read(void (*callback)(void *, async_error_code, size_t), void *context,
						const cancellation_slot &slot, void (*destroy)(void *));
	int async_write_some(const void *buf, size_t size,
						 void (*callback)(void *, async_error_code, size_t),
						 void *context);
	int async_write_some(const void *buf, size_t size,
						 void (*callback)(void *, async_error_code, size_t),
						 void *context, void (*destroy)(void *));
	int async_write_some(const void *buf, size_t size,
						 void (*callback)(void *, async_error_code, size_t), void *context,
						 const cancellation_slot &slot, void (*destroy)(void *));
	int async_writev_some(const struct iovec *iov, int iovcnt,
						  void (*callback)(void *, async_error_code, size_t),
						  void *context);
	int async_writev_some(const struct iovec *iov, int iovcnt,
						  void (*callback)(void *, async_error_code, size_t),
						  void *context, void (*destroy)(void *));
	int async_writev_some(const struct iovec *iov, int iovcnt,
						  void (*callback)(void *, async_error_code, size_t), void *context,
						  const cancellation_slot &slot, void (*destroy)(void *));
	/* Synchronous non-blocking write used by Workflow feedback(). */
	int write_some(const void *buf, size_t size);
	int cancel();
	int cancel_connect();
	int cancel_read();
	int cancel_write();
	SOCKET native_handle() const;
	executor get_executor() const;

	class impl;

private:
	impl *impl_;
};

#endif /* _ASYNC_TCP_SOCKET_H_ */

