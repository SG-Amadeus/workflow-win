/*
  AsyncCore: non-template stream and datagram message helpers.

  This is the simplified equivalent of the Runtime TCP read_message state
  machine.  It reads until the user filter accepts a complete message.
*/

#ifndef _ASYNC_MESSAGE_H_
#define _ASYNC_MESSAGE_H_

#include "../tcp_socket.h"
#include "../udp_socket.h"

#include <stddef.h>

class ssl_stream;

typedef int (*read_message_filter)(void *buffer, size_t *size, void *userdata);

int async_read_message(tcp_socket *socket,
					   void *buffer, size_t size,
					   read_message_filter filter, void *filter_userdata,
					   void (*callback)(void *, async_error_code, size_t),
					   void *context,
					   int timeout_ms = 0);

int async_read_message_ex(tcp_socket *socket,
						  void *buffer, size_t size,
						  read_message_filter filter, void *filter_userdata,
						  void (*callback)(void *, async_error_code, size_t),
						  void *context, int first_timeout_ms,
						  int next_timeout_ms, int total_timeout_ms,
						  cancellation_slot slot = cancellation_slot(),
						  void (*destroy)(void *) = nullptr,
						  volatile LONG *renew_flag = nullptr);

int async_read_message_ssl(ssl_stream *socket,
						   void *buffer, size_t size,
						   read_message_filter filter, void *filter_userdata,
						   void (*callback)(void *, async_error_code, size_t),
						   void *context,
						   int timeout_ms = 0);

int async_read_message_ssl_ex(ssl_stream *socket,
							  void *buffer, size_t size,
							  read_message_filter filter, void *filter_userdata,
							  void (*callback)(void *, async_error_code, size_t),
							  void *context, int first_timeout_ms,
							  int next_timeout_ms, int total_timeout_ms,
							  cancellation_slot slot = cancellation_slot(),
							  void (*destroy)(void *) = nullptr,
							  volatile LONG *renew_flag = nullptr);

int async_recvfrom_message(udp_socket *socket,
						   void *buffer, size_t size,
						   struct sockaddr *addr, int *addrlen,
						   read_message_filter filter, void *filter_userdata,
						   void (*callback)(void *, async_error_code, size_t),
						   void *context,
						   int timeout_ms = 0);

int async_recvfrom_message_ex(udp_socket *socket,
							  void *buffer, size_t size,
							  struct sockaddr *addr, int *addrlen,
							  read_message_filter filter,
							  void *filter_userdata,
							  void (*callback)(void *, async_error_code, size_t),
							  void *context, int timeout_ms,
							  cancellation_slot slot,
							  void (*destroy)(void *));

#endif /* _ASYNC_MESSAGE_H_ */

