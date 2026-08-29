cmake_minimum_required(VERSION 3.6)
project(kernel)

set(SRC
	# Workflow kernel
	rbtree.c
	msgqueue.cc
	thrdpool.cc
	CommRequest.cc
	CommScheduler.cc
	Executor.cc
	SubTask.cc
	# Async core objects
	async/executor.cc
	async/io_context.cc
	async/async_kernel.cc
	async/random_access_handle.cc
	async/op/message.cc
	async/steady_timer.cc
	async/strand.cc
	async/ssl_stream.cc
	async/tcp_acceptor.cc
	async/tcp_socket.cc
	async/udp_socket.cc
	# Async services
	async/service/socket_service.cc
	async/service/handle_service.cc
	async/service/handler_work.cc
	async/service/timer_queue.cc
	async/service/timer_queue_set.cc
	# Async operations
	async/op/cancellation.cc
	async/op/executor_op.cc
	async/op/iocp_op_cancellation.cc
	async/op/async_handler.cc
	async/op/composed_op.cc
	async/op/connect_op.cc
	async/op/recv_op.cc
	async/op/send_op.cc
	async/op/accept_op.cc
	async/op/recvfrom_op.cc
	async/op/sendto_op.cc
	async/op/write_op.cc
	async/op/read_op.cc
	async/op/read_until_op.cc
	async/op/handle_read_op.cc
	async/op/handle_write_op.cc
	async/op/handle_readv_op.cc
	async/op/handle_writev_op.cc
	async/op/timer_wait_op.cc
	async/op/op_pools.cc
	async/op/timed_connect_op.cc
	async/op/timed_write_op.cc
	async/op/timed_handshake_op.cc
	async/op/timed_udp_send_op.cc
	async/op/timed_ssl_connect_op.cc
	async/op/timed_ssl_write_op.cc
	async/op/read_message_ctx.cc
	async/op/timed_accept_op.cc
	async/op/recvfrom_message_ctx.cc
	# Communicator adaptation
	comm/comm_conn.cc
	comm/comm_request_op.cc
	comm/comm_sleep_op.cc
	comm/comm_file_io_op.cc
	comm/comm_service_op.cc
	Communicator.cc
	IOService.cc
)

add_library(${PROJECT_NAME} OBJECT ${SRC})
if (WIN32 AND NOT MINGW)
	target_compile_definitions(
		${PROJECT_NAME} PRIVATE
		strdup=_strdup
		strcasecmp=_stricmp
		strncasecmp=_strnicmp
	)
endif ()
