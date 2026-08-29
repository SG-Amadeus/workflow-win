/*
  AsyncCore: non-template socket service.

  Faithful non-template port of asio::detail::win_iocp_socket_service_base.
  The service owns the open-socket registry and the native overlapped
  initiation paths for connect, accept, send and receive.

  The resource impl still owns executor/state/cancel-token/op slots; the
  service owns the handle registry and IOCP binding.
*/

#ifndef _ASYNC_SOCKET_SERVICE_H_
#define _ASYNC_SOCKET_SERVICE_H_

#include <WinSock2.h>
#include <Windows.h>
#include <mswsock.h>
#include <ws2tcpip.h>

#include "../../list.h"

class io_context;
class win_iocp_operation;

class socket_impl
{
public:
	SOCKET socket_;
	DWORD safe_cancellation_thread_id_;
	LPFN_CONNECTEX connect_ex_;
	bool connect_ex_unavailable_;
	struct list_head registry_node_;

	explicit socket_impl()
		: socket_(INVALID_SOCKET),
		  safe_cancellation_thread_id_(0),
		  connect_ex_(nullptr),
		  connect_ex_unavailable_(false)
	{
		INIT_LIST_HEAD(&registry_node_);
	}

	~socket_impl() {}
};

class socket_service
{
public:
	explicit socket_service(io_context *io);
	~socket_service();

	socket_service(const socket_service &) = delete;
	socket_service &operator=(const socket_service &) = delete;

	/* Bind the native socket to the IOCP and add it to the registry. */
	int register_socket(socket_impl *impl, SOCKET socket);

	/* Remove the socket from the registry.  The caller remains responsible
	 * for closing the native handle. */
	void unregister_socket(socket_impl *impl);
	int cancel(socket_impl *impl);
	void start_connect_op(socket_impl *impl,
						  const struct sockaddr *addr, int addrlen,
						  win_iocp_operation *op);
	void start_accept_op(socket_impl *impl, int family, int type,
						 int protocol, SOCKET *accepted, void *buffer,
						 DWORD address_length, win_iocp_operation *op);
	void start_receive_op(socket_impl *impl, WSABUF *buffers, DWORD count,
						  DWORD flags, bool noop,
						  win_iocp_operation *op);
	void start_send_op(socket_impl *impl, WSABUF *buffers, DWORD count,
						 DWORD flags, bool noop,
						 win_iocp_operation *op);
	void start_receive_from_op(socket_impl *impl, WSABUF *buffers, DWORD count,
							   DWORD flags, struct sockaddr *addr, int *addrlen,
							   win_iocp_operation *op);
	void start_send_to_op(socket_impl *impl, WSABUF *buffers, DWORD count,
						  DWORD flags, const struct sockaddr *addr, int addrlen,
						  win_iocp_operation *op);

	/* Close every registered socket and clear the registry.  Used by
	 * io_context::shutdown(). */
	void shutdown();

	bool is_registered(socket_impl *impl) const;

private:
	void update_cancellation_thread_id(socket_impl *impl);
	io_context *io_;
	CRITICAL_SECTION lock_;
	struct list_head sockets_;
};

#endif /* _ASYNC_SOCKET_SERVICE_H_ */

