/*
  AsyncCore: minimal non-template random access file handle, named after ASIO.

  This is the simplified equivalent of asio::windows::random_access_handle.
  Each overlapped operation owns a separate operation object and may remain
  outstanding until its IOCP completion is dispatched.

  Completion callbacks are posted through the associated executor, so a
  strand-backed handle keeps file handlers serialized.
*/

#ifndef _ASYNC_RANDOM_ACCESS_HANDLE_H_
#define _ASYNC_RANDOM_ACCESS_HANDLE_H_

#include "executor.h"

#include <stddef.h>
#include <stdint.h>

#include "../../PlatformSocket.h"

class random_access_handle
{
public:
	random_access_handle();
	~random_access_handle();
	static random_access_handle *create(executor ex);
	static void destroy(random_access_handle *handle);
	int init(executor ex);

	random_access_handle(const random_access_handle &) = delete;
	random_access_handle &operator=(const random_access_handle &) = delete;
	int assign(HANDLE handle);
	/* Workflow file requests may bind a synchronous HANDLE. */
	int assign(HANDLE handle, bool overlapped);
	int async_read_some_at(uint64_t offset, void *buf, size_t size,
						   void (*callback)(void *, async_error_code, size_t),
						   void *context);
	int async_read_some_at(uint64_t offset, void *buf, size_t size,
						   void (*callback)(void *, async_error_code, size_t),
						   void *context, void (*destroy)(void *));
	int async_write_some_at(uint64_t offset, const void *buf, size_t size,
							void (*callback)(void *, async_error_code, size_t),
							void *context);
	int async_write_some_at(uint64_t offset, const void *buf, size_t size,
							void (*callback)(void *, async_error_code, size_t),
							void *context, void (*destroy)(void *));
	int async_readv_at(uint64_t offset, const struct iovec *iov, int iovcnt,
					   void (*callback)(void *, async_error_code, size_t),
					   void *context);
	int async_readv_at(uint64_t offset, const struct iovec *iov, int iovcnt,
					   void (*callback)(void *, async_error_code, size_t),
					   void *context, void (*destroy)(void *));
	int async_writev_at(uint64_t offset, const struct iovec *iov, int iovcnt,
						void (*callback)(void *, async_error_code, size_t),
						void *context);
	int async_writev_at(uint64_t offset, const struct iovec *iov, int iovcnt,
						void (*callback)(void *, async_error_code, size_t),
						void *context, void (*destroy)(void *));
	int async_fsync(void (*callback)(void *, async_error_code, size_t), void *context,
					void (*destroy)(void *) = nullptr);
	int cancel();
	int release();
	int close();
	HANDLE native_handle() const;
	executor get_executor() const;
	bool is_blocking() const;

	class impl;

private:
	impl *impl_;
};

#endif /* _ASYNC_RANDOM_ACCESS_HANDLE_H_ */

