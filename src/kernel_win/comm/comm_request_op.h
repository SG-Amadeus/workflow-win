/*
 * AsyncCore: composed operation for a Communicator request flow.
 *
 * Each request/reply is one composed operation driving connect/handshake/
 * write/read children.  The connection object (CommConnEntry) is persistent:
 * it owns transport resources and keep-alive state.  Server keep-alive uses
 * an inbound read; client pooled connections use only an expiry timer.  An
 * operation never hands the connection off to another operation without an
 * entry-strand activation step.
 */

#ifndef _ASYNC_OP_COMM_REQUEST_OP_H_
#define _ASYNC_OP_COMM_REQUEST_OP_H_

#include <WinSock2.h>
#include <stddef.h>

#include "../async/op/composed_op.h"
#include "comm_conn.h"

#include "../thrdpool.h"
#include "../async/op/composed_operations.h"

class comm_request_op : public composed_op
{
	public:
	enum request_kind
	{
		REQUEST_CLIENT,
		REQUEST_SERVER_START,
		REQUEST_SERVER_REPLY,
		REQUEST_SERVER_RECEIVE
	};

	CommConnEntry *entry_;
	CommSession *session_;
	struct sockaddr_storage addr_storage_;
	const struct sockaddr *addr_;
	int addrlen_;
	bool reuse_;
	request_kind kind_;
	bool keep_alive_;
	bool entry_ref_;
	bool terminal_;
	int business_error_;
	volatile LONG renew_requested_;
	struct thrdpool_task_entry handler_task_;

	comm_request_op();

	static void destroy(composed_op *base);
	static void complete(composed_op *base);
	static void cancel(composed_op *base, cancellation_type type);
	static void finish(comm_request_op *self, int state, int error);
	static void start_flow(comm_request_op *self);

	static void connect_cb(void *ctx, async_error_code error);
	static void handshake_cb(void *ctx, async_error_code error);
	static void server_handshake_cb(void *ctx, async_error_code error);
	static void write_cb(void *ctx, async_error_code error, size_t bytes);
	static void read_cb(void *ctx, async_error_code error, size_t bytes);
	static void server_read_cb(void *ctx, async_error_code error, size_t bytes);
	static void server_read_destroy(void *ctx);
	static void udp_send_cb(void *ctx, async_error_code error, size_t bytes);
	static void read_destroy(void *ctx);
	static void child_destroy(void *ctx);
	static void handle_complete(void *ctx);
	static void post_completion(comm_request_op *self);
	static void retire_io(comm_request_op *self, int state, int error);
	static int request_idle_entry(CommSession *session, CommTarget *target);
	static int start_client(CommunicatorImpl *impl, CommSession *session,
							CommTarget *target);
	static int drain_service(CommService *service, int max);
	static int shutdown_request(CommConnEntry *entry);
	static void destroy_request(CommConnEntry *entry);
	static void destroy_session(CommSession *session);
	static void cancel_entry(void *context);
	static void release_entry(void *context);
	static void activate_client_entry(void *context);
	static int client_ssl_init(SSL *ssl, void *userdata);
	static int server_ssl_init(SSL *ssl, void *userdata);
	static int start_reply(CommConnEntry *entry, const struct sockaddr *addr,
						   int addrlen, bool reserved = false);
	static int start_server(CommConnEntry *entry);
	static int start_server_datagram(CommConnEntry *entry,
									const void *buffer, size_t bytes);
	static void complete_server_reply(comm_request_op *self);
	static int create_server_session(CommConnEntry *entry);
	static void retire_idle_entry(CommConnEntry *entry);
	static void retire_request(CommConnEntry *entry, int state, int error,
								   bool notify, bool clear_input = true);
	static int prepare_message_out(CommConnEntry *entry,
								 struct iovec *vectors, int *count);
	static int prepare_message_in(CommConnEntry *entry);
	static int read_message_filter(void *buffer, size_t *size,
								   void *userdata);
	static int park_client_entry(CommConnEntry *entry, int timeout);
	static int start_server_read(CommConnEntry *entry, int first_timeout);
	static int start_idle_timer(CommConnEntry *entry, int timeout);
	static void idle_timer_cb(void *ctx, async_error_code error);
	static void idle_timer_destroy(void *ctx);
	static comm_request_op *create(CommConnEntry *entry, request_kind kind,
								 bool hold_entry);

private:
	static int start_send(comm_request_op *self);
	static int start_read(comm_request_op *self);
	static int start_udp_read(comm_request_op *self);
	static int start_message_read(comm_request_op *self,
								  void (*callback)(void *, async_error_code, size_t),
								  void (*destroy)(void *),
								  int first_timeout);
};

int comm_request_start(CommConnEntry *entry, const struct sockaddr *addr,
					   int addrlen, bool reuse);

#endif /* _ASYNC_OP_COMM_REQUEST_OP_H_ */
