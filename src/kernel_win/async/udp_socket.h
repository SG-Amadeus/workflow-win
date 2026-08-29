/*
  AsyncCore: minimal non-template UDP socket, named after ASIO.

  This is the simplified equivalent of asio::ip::udp::socket.
  One receive and one send may be outstanding at the same time, matching
  ASIO's documented socket concurrency model.
*/

#ifndef _ASYNC_UDP_SOCKET_H_
#define _ASYNC_UDP_SOCKET_H_

#include "executor.h"
#include "op/cancellation.h"

#include <WinSock2.h>

#include <stddef.h>

#include "../../PlatformSocket.h"

class udp_socket
{
public:
	udp_socket();
	~udp_socket();
	static udp_socket *create(executor ex);
	static void destroy(udp_socket *socket);
	int init(executor ex);

	udp_socket(const udp_socket &) = delete;
	udp_socket &operator=(const udp_socket &) = delete;
	int open();
	int open(int family);
	int assign(SOCKET socket);
	int bind(const struct sockaddr *addr, int addrlen);
	int close();

	int async_receive_from(void *buf, size_t size,
						   struct sockaddr *addr, int *addrlen,
						   void (*callback)(void *, async_error_code, size_t),
						   void *context);
	int async_receive_from(void *buf, size_t size,
						   struct sockaddr *addr, int *addrlen,
						   void (*callback)(void *, async_error_code, size_t),
						   void *context, void (*destroy)(void *));
	int async_receive_from(void *buf, size_t size,
						   struct sockaddr *addr, int *addrlen,
						   void (*callback)(void *, async_error_code, size_t), void *context,
						   const cancellation_slot &slot, void (*destroy)(void *));
	int async_send_to(const void *buf, size_t size,
					  const struct sockaddr *addr, int addrlen,
					  void (*callback)(void *, async_error_code, size_t),
					  void *context);
	int async_send_to(const void *buf, size_t size,
					  const struct sockaddr *addr, int addrlen,
					  void (*callback)(void *, async_error_code, size_t),
					  void *context, void (*destroy)(void *));
	int async_send_to(const void *buf, size_t size,
					  const struct sockaddr *addr, int addrlen,
					  void (*callback)(void *, async_error_code, size_t), void *context,
					  const cancellation_slot &slot, void (*destroy)(void *));
	int async_sendto_v(const struct iovec *iov, int iovcnt,
					   const struct sockaddr *addr, int addrlen,
					   void (*callback)(void *, async_error_code, size_t),
					   void *context);
	int async_sendto_v(const struct iovec *iov, int iovcnt,
					   const struct sockaddr *addr, int addrlen,
					   void (*callback)(void *, async_error_code, size_t),
					   void *context, void (*destroy)(void *));
	int async_sendto_v(const struct iovec *iov, int iovcnt,
					   const struct sockaddr *addr, int addrlen,
					   void (*callback)(void *, async_error_code, size_t), void *context,
					   const cancellation_slot &slot, void (*destroy)(void *));
	/* Synchronous non-blocking send used by Workflow feedback(). */
	int send_to(const void *buf, size_t size,
				const struct sockaddr *addr, int addrlen);
	int cancel();
	int cancel_read();
	int cancel_write();
	SOCKET native_handle() const;
	executor get_executor() const;

	class impl;

private:
	impl *impl_;
};

#endif /* _ASYNC_UDP_SOCKET_H_ */

