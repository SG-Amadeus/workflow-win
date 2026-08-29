/*
  AsyncCore: recvfrom-message composed state, non-template.
*/

#ifndef _ASYNC_OP_RECVFROM_MESSAGE_CTX_H_
#define _ASYNC_OP_RECVFROM_MESSAGE_CTX_H_

#include "../executor.h"
#include "../steady_timer.h"
#include "../udp_socket.h"
#include "message.h"

#include <WinSock2.h>
#include <Windows.h>

#include <stddef.h>

int async_start_recvfrom_message(udp_socket *socket,
								 void *buffer, size_t size,
								 struct sockaddr *addr, int *addrlen,
								 read_message_filter filter,
								 void *filter_userdata,
								 void (*callback)(void *, async_error_code, size_t),
								 void *context, int timeout_ms,
								 const cancellation_slot &slot,
								 void (*destroy)(void *),
								 bool exact_timeout);

class recvfrom_message_ctx
{
public:
	udp_socket *socket;
	void *buffer;
	void *owned_buffer;
	size_t size;
	read_message_filter filter;
	void *filter_userdata;
	void (*callback)(void *, async_error_code, size_t);
	void (*destroy)(void *);
	void *context;
	executor executor_;
	async_error_code result_error;
	size_t result_bytes;
	volatile LONG destroy_called;
	struct sockaddr *addr;
	int *addrlen;
	struct sockaddr_storage owned_addr;
	int owned_addrlen;
	steady_timer *timer;
	int timeout_ms;
	volatile LONG refs;
	volatile LONG done;
	cancellation_state cancel_state;
	cancellation_signal read_cancel;
	CRITICAL_SECTION mutex_;
};

#endif /* _ASYNC_OP_RECVFROM_MESSAGE_CTX_H_ */

