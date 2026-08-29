/*
  AsyncCore: non-template TCP acceptor, ported from ASIO win_iocp.

  This is the equivalent of asio::ip::tcp::acceptor.
  Only one accept operation may be outstanding at a time.

  The implementation uses AcceptEx as an IOCP operation, matching ASIO's
  win_iocp_socket_service_base::start_accept_op.
*/

#ifndef _ASYNC_TCP_ACCEPTOR_H_
#define _ASYNC_TCP_ACCEPTOR_H_

#include "executor.h"
#include "op/cancellation.h"

#include <WinSock2.h>

class tcp_acceptor
{
public:
	tcp_acceptor();
	~tcp_acceptor();
	static tcp_acceptor *create(executor ex);
	static void destroy(tcp_acceptor *acceptor);
	int init(executor ex);

	tcp_acceptor(const tcp_acceptor &) = delete;
	tcp_acceptor &operator=(const tcp_acceptor &) = delete;
	int open();
	int open(int family);
	int assign(SOCKET listener);
	int bind(const struct sockaddr *addr, int addrlen);
	int listen(int backlog);
	int close();

	int async_accept(void (*callback)(void *, async_error_code, SOCKET), void *context);
	int async_accept(void (*callback)(void *, async_error_code, SOCKET), void *context,
					 void (*destroy)(void *));
	int async_accept(void (*callback)(void *, async_error_code, SOCKET), void *context,
					 const cancellation_slot &slot, void (*destroy)(void *));
	int cancel();
	SOCKET native_handle() const;
	executor get_executor() const;

	class impl;

private:
	impl *impl_;
};

#endif /* _ASYNC_TCP_ACCEPTOR_H_ */

