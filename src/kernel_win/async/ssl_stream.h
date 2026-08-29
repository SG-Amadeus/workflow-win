/*
  AsyncCore: minimal non-template SSL stream, named after ASIO.

  This is the simplified equivalent of asio::ssl::stream<tcp_socket>.
  It is a memory-BIO composed operation over tcp_socket, following the
  ASIO SSL engine/win_iocp_operation shape without templates.
*/

#ifndef _ASYNC_SSL_STREAM_H_
#define _ASYNC_SSL_STREAM_H_

#include "executor.h"
#include "op/cancellation.h"

#include <WinSock2.h>

#include <stddef.h>

#include "../../PlatformSocket.h"

struct ssl_ctx_st;
struct ssl_st;

typedef int (*ssl_stream_init_callback)(struct ssl_st *ssl, void *userdata);

class ssl_stream
{
public:
	ssl_stream();
	~ssl_stream();
	static ssl_stream *create(executor ex, struct ssl_ctx_st *ctx, int server);
	static void destroy(ssl_stream *stream);
	int init(executor ex, struct ssl_ctx_st *ctx, int server);

	ssl_stream(const ssl_stream &) = delete;
	ssl_stream &operator=(const ssl_stream &) = delete;
	int assign(SOCKET socket);
	int set_init_callback(ssl_stream_init_callback cb, void *userdata);
	int set_server(int server);
	int async_connect(const struct sockaddr *addr, int addrlen,
					  void (*callback)(void *, async_error_code), void *context);
	int async_connect(const struct sockaddr *addr, int addrlen,
					  void (*callback)(void *, async_error_code), void *context,
					  void (*destroy)(void *));
	int async_connect_transport(const struct sockaddr *addr, int addrlen,
								void (*callback)(void *, async_error_code), void *context,
								void (*destroy)(void *) = nullptr);
	int async_connect_transport(const struct sockaddr *addr, int addrlen,
								void (*callback)(void *, async_error_code), void *context,
								const cancellation_slot &slot,
								void (*destroy)(void *));
	int async_handshake(void (*callback)(void *, async_error_code), void *context);
	int async_handshake(void (*callback)(void *, async_error_code), void *context,
						void (*destroy)(void *));
	int async_handshake(void (*callback)(void *, async_error_code), void *context,
						const cancellation_slot &slot, void (*destroy)(void *));
	int async_shutdown(void (*callback)(void *, async_error_code), void *context);
	int async_shutdown(void (*callback)(void *, async_error_code), void *context,
					   void (*destroy)(void *));
	int async_shutdown(void (*callback)(void *, async_error_code), void *context,
					   const cancellation_slot &slot, void (*destroy)(void *));
	int async_read_some(void *buf, size_t size,
						void (*callback)(void *, async_error_code, size_t),
						void *context);
	int async_read_some(void *buf, size_t size,
						void (*callback)(void *, async_error_code, size_t),
						void *context, void (*destroy)(void *));
	int async_read_some(void *buf, size_t size,
						void (*callback)(void *, async_error_code, size_t), void *context,
						const cancellation_slot &slot, void (*destroy)(void *));
	int async_write_some(const void *buf, size_t size,
						 void (*callback)(void *, async_error_code, size_t),
						 void *context);
	int async_write_some(const void *buf, size_t size,
						 void (*callback)(void *, async_error_code, size_t),
						 void *context, void (*destroy)(void *));
	int async_write_some(const void *buf, size_t size,
						 void (*callback)(void *, async_error_code, size_t), void *context,
						 const cancellation_slot &slot, void (*destroy)(void *));
	int async_writev_some(const struct iovec *iov, int iovcnt,
						  void (*callback)(void *, async_error_code, size_t),
						  void *context);
	int async_writev_some(const struct iovec *iov, int iovcnt,
						  void (*callback)(void *, async_error_code, size_t),
						  void *context, void (*destroy)(void *));
	int async_writev_some(const struct iovec *iov, int iovcnt,
						  void (*callback)(void *, async_error_code, size_t), void *context,
						  const cancellation_slot &slot, void (*destroy)(void *));
	/* Send ciphertext already produced by the Workflow SSL wrapper. */
	int write_transport_some(const void *buf, size_t size);
	int async_wait_read(void (*callback)(void *, async_error_code, size_t),
						void *context, void (*destroy)(void *) = nullptr);
	int async_wait_read(void (*callback)(void *, async_error_code, size_t), void *context,
						const cancellation_slot &slot, void (*destroy)(void *));
	int cancel();
	int close();
	SOCKET native_handle() const;
	executor get_executor() const;

	class impl;

private:
	impl *impl_;
};

#endif /* _ASYNC_SSL_STREAM_H_ */


