/*
 * AsyncCore: ASIO-style composed operation for Communicator file I/O.
 *
 * This op owns the FileIOContext and delivers IOSession completion/destroy
 * callbacks.  It is the async/op counterpart of asio::windows::random_access_
 * handle async operations with business completion.
 */

#ifndef _ASYNC_OP_COMM_FILE_IO_OP_H_
#define _ASYNC_OP_COMM_FILE_IO_OP_H_

#include "../async/op/composed_op.h"
#include "comm_conn.h"

#include "../thrdpool.h"

class comm_file_io_op : public composed_op
{
	public:
	FileIOContext *ctx_;
	IOSession *session_;
	IOService *service_;
	volatile LONG context_released_;
	volatile LONG service_released_;
	struct thrdpool_task_entry handler_task_;

	comm_file_io_op();

	static void destroy(composed_op *base);
	static void complete(composed_op *base);
	static void file_cb(void *ctx, async_error_code error, size_t bytes);
	static void file_destroy(void *ctx);
	static void handle_complete(void *ctx);
	static void post_completion(comm_file_io_op *self,
								 async_error_code error, size_t bytes);
	static void release_context_data(FileIOContext *ctx, bool release_service);
	static void release_context(comm_file_io_op *self);
	static void release_service(comm_file_io_op *self);
	static int start(FileIOContext *ctx);
	static int start(CommunicatorImpl *impl, IOSession *session);
};

#endif /* _ASYNC_OP_COMM_FILE_IO_OP_H_ */


