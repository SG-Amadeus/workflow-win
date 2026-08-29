/*
  AsyncCore: non-template handle service.

  Faithful non-template port of asio::detail::win_iocp_handle_service.
  The service owns the open-handle registry and the ReadFile/WriteFile
  initiation path for overlapped operations.
*/

#ifndef _ASYNC_HANDLE_SERVICE_H_
#define _ASYNC_HANDLE_SERVICE_H_

#include <WinSock2.h>
#include <Windows.h>
#include <stdint.h>

#include "../../list.h"
#include "../op/win_iocp_operation.h"

class io_context;

class handle_impl
{
public:
	HANDLE handle_;
	DWORD safe_cancellation_thread_id_;
	struct list_head registry_node_;

	handle_impl()
		: handle_(INVALID_HANDLE_VALUE),
		  safe_cancellation_thread_id_(0)
	{
		INIT_LIST_HEAD(&registry_node_);
	}

	~handle_impl() {}
};

class handle_service
{
public:
	explicit handle_service(io_context *io);
	~handle_service();

	handle_service(const handle_service &) = delete;
	handle_service &operator=(const handle_service &) = delete;

	/* Register an implementation.  ASIO always binds a handle to IOCP;
	 * Workflow's non-overlapped file path deliberately registers the handle
	 * without that binding and executes its blocking operation on the file
	 * pool.  Both paths still share the service-owned lifetime registry. */
	int register_handle(handle_impl *impl, HANDLE handle, bool associate_iocp);
	void unregister_handle(handle_impl *impl);
	int cancel(handle_impl *impl);
	void start_read_op(handle_impl *impl, uint64_t offset, void *buffer,
						size_t size, win_iocp_operation *op);
	void start_write_op(handle_impl *impl, uint64_t offset, const void *buffer,
						size_t size, win_iocp_operation *op);
	void shutdown();
	bool is_registered(handle_impl *impl) const;

private:
	void update_cancellation_thread_id(handle_impl *impl);
	io_context *io_;
	CRITICAL_SECTION lock_;
	struct list_head handles_;
};

#endif /* _ASYNC_HANDLE_SERVICE_H_ */

