/*
 * AsyncCore: ASIO composed operation for Communicator service accept/recv loops.
 *
 * This op owns the service accept (TCP) and receive-from (UDP) loops.  Each
 * accepted datagram/connection is handed to the business layer through the
 * normal CommSession path, while the service loop continues to re-arm itself.
 */

#ifndef _ASYNC_OP_COMM_SERVICE_OP_H_
#define _ASYNC_OP_COMM_SERVICE_OP_H_

#include "comm_conn.h"

#include "../thrdpool.h"

class comm_service_op
{
public:
	struct accept_context
	{
		comm_service_op *op;
		volatile LONG released;
	};

	CommService *service_;
	UdpServiceContext *udp_ctx_;
	TcpServiceContext *tcp_ctx_;
	bool udp_;
	volatile LONG refs_;
	volatile LONG handler_pending_;
	volatile LONG result_error_;
	struct thrdpool_task_entry handler_task_;

	comm_service_op();
	void acquire();
	void release();
	static int bind(CommunicatorImpl *impl, CommService *service);
	static void unbind(CommService *service);
	static int start(CommService *service);
	static int arm(comm_service_op *self);

	static void accept_cb(void *ctx, async_error_code error, SOCKET socket);
	static void accept_destroy(void *ctx);
	static void release_accept_callback(accept_context *context);
	static void udp_recv_cb(void *ctx, async_error_code error, size_t bytes);
	static void udp_recv_destroy(void *ctx);
	static void handle_complete(void *ctx);
	static void post_completion(comm_service_op *self, int error);
	static void destroy(comm_service_op *self);
};

#endif /* _ASYNC_OP_COMM_SERVICE_OP_H_ */


