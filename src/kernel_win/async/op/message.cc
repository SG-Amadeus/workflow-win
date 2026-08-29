#include "message.h"
#include "../ssl_stream.h"
#include "../steady_timer.h"
#include "read_message_ctx.h"
#include "recvfrom_message_ctx.h"

#include <errno.h>
#include <limits.h>
#include <new>
#include <stdlib.h>
#include <string.h>
#include <windows.h>

namespace
{

static int tcp_submit(void *socket, void *buf, size_t size,
					  void (*callback)(void *, async_error_code, size_t), void *context,
					  const cancellation_slot &slot,
					  void (*destroy)(void *))
{
	return static_cast<tcp_socket *>(socket)->async_read_some(
		buf, size, callback, context, slot, destroy);
}

static int ssl_submit(void *socket, void *buf, size_t size,
					  void (*callback)(void *, async_error_code, size_t), void *context,
					  const cancellation_slot &slot,
					  void (*destroy)(void *))
{
	return static_cast<ssl_stream *>(socket)->async_read_some(
		buf, size, callback, context, slot, destroy);
}

static executor tcp_executor(void *socket)
{
	return static_cast<tcp_socket *>(socket)->get_executor();
}

static executor ssl_executor(void *socket)
{
	return static_cast<ssl_stream *>(socket)->get_executor();
}

} /* namespace */

int async_read_message_ex(tcp_socket *socket, void *buffer, size_t size,
						  read_message_filter filter, void *filter_userdata,
						  void (*callback)(void *, async_error_code, size_t), void *context,
						  int first_timeout, int next_timeout, int total_timeout,
						  cancellation_slot slot,
						  void (*destroy)(void *), volatile LONG *renew_flag)
{
	return async_start_read_message(socket, tcp_submit, tcp_executor,
		buffer, size, filter, filter_userdata, callback, context,
		first_timeout, next_timeout, total_timeout, slot, destroy, renew_flag);
}

int async_read_message_ssl_ex(ssl_stream *socket, void *buffer, size_t size,
							  read_message_filter filter, void *filter_userdata,
							  void (*callback)(void *, async_error_code, size_t), void *context,
							  int first_timeout, int next_timeout,
							  int total_timeout, cancellation_slot slot,
							  void (*destroy)(void *),
							  volatile LONG *renew_flag)
{
	return async_start_read_message(socket, ssl_submit, ssl_executor,
		buffer, size, filter, filter_userdata, callback, context,
		first_timeout, next_timeout, total_timeout, slot, destroy, renew_flag);
}

int async_read_message(tcp_socket *socket, void *buffer, size_t size,
					   read_message_filter filter, void *filter_userdata,
					   void (*callback)(void *, async_error_code, size_t), void *context,
					   int timeout)
{
	return async_read_message_ex(socket, buffer, size, filter, filter_userdata,
		callback, context, 0, -1, timeout > 0 ? timeout : -1,
		cancellation_slot(), nullptr, nullptr);
}

int async_read_message_ssl(ssl_stream *socket, void *buffer, size_t size,
						   read_message_filter filter, void *filter_userdata,
						   void (*callback)(void *, async_error_code, size_t), void *context,
						   int timeout)
{
	return async_read_message_ssl_ex(socket, buffer, size, filter,
		filter_userdata, callback, context, 0, -1,
		timeout > 0 ? timeout : -1, cancellation_slot(), nullptr, nullptr);
}

int async_recvfrom_message(udp_socket *socket,
						   void *buffer, size_t size,
						   struct sockaddr *addr, int *addrlen,
						   read_message_filter filter, void *filter_userdata,
						   void (*callback)(void *, async_error_code, size_t),
						   void *context, int timeout_ms)
{
	return async_start_recvfrom_message(socket, buffer, size, addr, addrlen,
		filter, filter_userdata, callback, context, timeout_ms,
		cancellation_slot(), nullptr, false);
}

int async_recvfrom_message_ex(udp_socket *socket,
							  void *buffer, size_t size,
							  struct sockaddr *addr, int *addrlen,
							  read_message_filter filter,
							  void *filter_userdata,
							  void (*callback)(void *, async_error_code, size_t),
							  void *context, int timeout_ms,
							  cancellation_slot slot,
							  void (*destroy)(void *))
{
	return async_start_recvfrom_message(socket, buffer, size, addr, addrlen,
		filter, filter_userdata, callback, context, timeout_ms, slot,
		destroy, true);
}

