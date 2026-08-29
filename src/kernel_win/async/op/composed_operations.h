/*
 * Public async composition surface for the Workflow adapter.
 *
 * Communicator uses these fixed, non-template operation entry points.  The
 * concrete composed state and its child operations remain private to async/op.
 */

#ifndef _ASYNC_COMPOSED_OPERATIONS_H_
#define _ASYNC_COMPOSED_OPERATIONS_H_

#include "cancellation.h"
#include "message.h"
#include "read_op.h"
#include "read_until_op.h"
#include "write_op.h"

#include <WinSock2.h>
#include <stdint.h>
#include <stddef.h>

class tcp_acceptor;
class tcp_socket;
class ssl_stream;
class udp_socket;
class random_access_handle;
class steady_timer;

int timed_accept_start(tcp_acceptor *acceptor, steady_timer *timer,
					   int timeout_ms, void (*destroy)(void *),
					   void (*callback)(void *, async_error_code, SOCKET), void *context,
					   cancellation_slot slot = cancellation_slot(),
					   void (*destroy_context)(void *) = nullptr);

int timed_connect_start(tcp_socket *socket, steady_timer *timer,
						const struct sockaddr *addr, int addrlen,
						int timeout_ms, void (*callback)(void *, async_error_code),
						void *context,
						cancellation_slot slot = cancellation_slot(),
						void (*destroy_context)(void *) = nullptr);

int timed_write_start(tcp_socket *socket, steady_timer *timer,
					  const struct iovec *iov, int iovcnt, int timeout_ms,
					  void (*callback)(void *, async_error_code, size_t), void *context,
					  cancellation_slot slot = cancellation_slot(),
					  void (*destroy_context)(void *) = nullptr);

int timed_ssl_connect_start(ssl_stream *stream, steady_timer *timer,
							const struct sockaddr *addr, int addrlen,
							int timeout_ms, void (*callback)(void *, async_error_code),
							void *context,
							cancellation_slot slot = cancellation_slot(),
							void (*destroy_context)(void *) = nullptr);

int timed_ssl_write_start(ssl_stream *stream, steady_timer *timer,
						  const struct iovec *iov, int iovcnt, int timeout_ms,
						  void (*callback)(void *, async_error_code, size_t), void *context,
						  cancellation_slot slot = cancellation_slot(),
						  void (*destroy_context)(void *) = nullptr);

int timed_handshake_start(ssl_stream *stream, steady_timer *timer,
						  int timeout_ms, void (*callback)(void *, async_error_code),
						  void *context,
						  cancellation_slot slot = cancellation_slot(),
						  void (*destroy_context)(void *) = nullptr);

int timed_udp_send_start(udp_socket *socket, steady_timer *timer,
						 const struct iovec *iov, int iovcnt,
						 const struct sockaddr *addr, int addrlen,
						 int timeout_ms,
						 void (*callback)(void *, async_error_code, size_t), void *context,
						 cancellation_slot slot = cancellation_slot(),
						 void (*destroy_context)(void *) = nullptr);

#endif /* _ASYNC_COMPOSED_OPERATIONS_H_ */

