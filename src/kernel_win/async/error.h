/*
 * Error values used by the asynchronous kernel.
 *
 * ASIO completion handlers receive std::error_code. The kernel keeps the
 * native Windows value and its category intact. Conversion to Workflow's
 * errno convention is explicit and belongs at the Comm boundary only.
 */
#ifndef _ASYNC_ERROR_H_
#define _ASYNC_ERROR_H_

#include <WinSock2.h>
#include <Windows.h>
#include <errno.h>
#include <system_error>

typedef std::error_code async_error_code;

enum async_misc_error
{
	async_error_eof = 1,
	async_error_not_found = 2
};

enum async_ssl_stream_error
{
	async_ssl_stream_truncated = 1,
	async_ssl_unspecified_system_error = 2,
	async_ssl_unexpected_result = 3
};

class async_misc_category_impl : public std::error_category
{
public:
	const char *name() const noexcept override
	{
		return "async.misc";
	}

	std::string message(int value) const override
	{
		switch (value)
		{
		case async_error_eof:
			return "End of file or stream";
		case async_error_not_found:
			return "Element not found";
		default:
			return "Async miscellaneous error";
		}
	}
};

class async_ssl_category_impl : public std::error_category
{
public:
	const char *name() const noexcept override
	{
		return "async.ssl";
	}

	std::string message(int) const override
	{
		return "OpenSSL error";
	}
};

class async_ssl_stream_category_impl : public std::error_category
{
public:
	const char *name() const noexcept override
	{
		return "async.ssl.stream";
	}

	std::string message(int value) const override
	{
		switch (value)
		{
		case async_ssl_stream_truncated:
			return "SSL stream truncated";
		case async_ssl_unspecified_system_error:
			return "SSL unspecified system error";
		case async_ssl_unexpected_result:
			return "SSL unexpected result";
		default:
			return "SSL stream error";
		}
	}
};

static inline const std::error_category &async_misc_category()
{
	static const async_misc_category_impl category;
	return category;
}

static inline const std::error_category &async_ssl_category()
{
	static const async_ssl_category_impl category;
	return category;
}

static inline const std::error_category &async_ssl_stream_category()
{
	static const async_ssl_stream_category_impl category;
	return category;
}

static inline async_error_code async_system_error(int value)
{
	return async_error_code(value, std::system_category());
}

/* On Windows ASIO's socket errors are native WSA values in system_category. */
static inline async_error_code async_socket_error(int value)
{
	return async_error_code(value, std::system_category());
}

static inline async_error_code async_generic_error(int value)
{
	return async_error_code(value, async_misc_category());
}

static inline async_error_code async_ssl_error(int value)
{
	return async_error_code(value, async_ssl_category());
}

static inline async_error_code async_ssl_stream_error(int value)
{
	return async_error_code(value, async_ssl_stream_category());
}

static inline async_error_code async_native_error(DWORD value)
{
	return async_system_error(static_cast<int>(value));
}

/* Used when a local initiation failure has to become an async completion. */
static inline async_error_code async_error_from_errno(int value)
{
	switch (value)
	{
	case 0:
		return async_error_code();
	case ECANCELED:
		return async_system_error(ERROR_OPERATION_ABORTED);
	case EACCES:
		return async_socket_error(WSAEACCES);
	case EADDRINUSE:
		return async_socket_error(WSAEADDRINUSE);
	case EADDRNOTAVAIL:
		return async_socket_error(WSAEADDRNOTAVAIL);
	case EAFNOSUPPORT:
		return async_socket_error(WSAEAFNOSUPPORT);
	case EALREADY:
		return async_socket_error(WSAEALREADY);
	case EBADF:
		return async_socket_error(WSAEBADF);
	case ETIMEDOUT:
		return async_socket_error(WSAETIMEDOUT);
	case EDESTADDRREQ:
		return async_socket_error(WSAEDESTADDRREQ);
	case EFAULT:
		return async_socket_error(WSAEFAULT);
	case EINPROGRESS:
		return async_socket_error(WSAEINPROGRESS);
	case EINTR:
		return async_socket_error(WSAEINTR);
	case ECONNRESET:
		return async_socket_error(WSAECONNRESET);
	case ECONNABORTED:
		return async_socket_error(WSAECONNABORTED);
	case ECONNREFUSED:
		return async_socket_error(WSAECONNREFUSED);
	case EISCONN:
		return async_socket_error(WSAEISCONN);
	case EMSGSIZE:
		return async_socket_error(WSAEMSGSIZE);
	case ENETDOWN:
		return async_socket_error(WSAENETDOWN);
	case ENETRESET:
		return async_socket_error(WSAENETRESET);
	case ENETUNREACH:
		return async_socket_error(WSAENETUNREACH);
	case ENOBUFS:
		return async_socket_error(WSAENOBUFS);
	case ENOPROTOOPT:
		return async_socket_error(WSAENOPROTOOPT);
	case ENOTCONN:
		return async_socket_error(WSAENOTCONN);
	case ENOTSOCK:
		return async_socket_error(WSAENOTSOCK);
	case EOPNOTSUPP:
		return async_socket_error(WSAEOPNOTSUPP);
	case EPROTONOSUPPORT:
		return async_socket_error(WSAEPROTONOSUPPORT);
	case EPROTOTYPE:
		return async_socket_error(WSAEPROTOTYPE);
	case EPIPE:
		return async_socket_error(WSAESHUTDOWN);
	case EAGAIN:
		return async_socket_error(WSAEWOULDBLOCK);
	case ENOENT:
		return async_native_error(ERROR_FILE_NOT_FOUND);
	case ENODEV:
		return async_native_error(ERROR_BAD_UNIT);
	case EPERM:
		return async_native_error(ERROR_ACCESS_DENIED);
	case ENOMEM:
		return async_system_error(ERROR_NOT_ENOUGH_MEMORY);
	case EINVAL:
		return async_system_error(ERROR_INVALID_PARAMETER);
	case EIO:
		return async_system_error(ERROR_GEN_FAILURE);
	default:
		return async_system_error(value);
	}
}

/* This is the only direction in which the Workflow adapter translates. */
static inline int async_error_to_errno(const async_error_code &error)
{
	if (!error)
		return 0;

	if (error.category() == async_misc_category())
	{
		if (error.value() == async_error_eof)
			return ECONNRESET;
		if (error.value() == async_error_not_found)
			return EMSGSIZE;
	}

	if (error.category() == async_ssl_stream_category())
		return EIO;

	int value = error.value();
	switch (static_cast<DWORD>(value))
	{
	case ERROR_OPERATION_ABORTED:
		return ECANCELED;
	case ERROR_INVALID_HANDLE:
	case WSAEBADF:
	case WSAENOTSOCK:
		return EBADF;
	case ERROR_INVALID_PARAMETER:
	case WSAEINVAL:
	case WSAEFAULT:
		return EINVAL;
	case ERROR_ACCESS_DENIED:
	case WSAEACCES:
		return EACCES;
	case ERROR_FILE_NOT_FOUND:
	case ERROR_PATH_NOT_FOUND:
		return ENOENT;
	case ERROR_NOT_ENOUGH_MEMORY:
	case ERROR_OUTOFMEMORY:
		return ENOMEM;
	case ERROR_BROKEN_PIPE:
		return EPIPE;
	case ERROR_NOT_SUPPORTED:
	case WSAEOPNOTSUPP:
		return EOPNOTSUPP;
	case WSAEINTR:
		return EINTR;
	case WSAEINPROGRESS:
		return EINPROGRESS;
	case WSAEALREADY:
		return EALREADY;
	case WSAEWOULDBLOCK:
		return EAGAIN;
	case WSAEADDRINUSE:
		return EADDRINUSE;
	case WSAEADDRNOTAVAIL:
		return EADDRNOTAVAIL;
	case WSAEAFNOSUPPORT:
		return EAFNOSUPPORT;
	case WSAECONNABORTED:
		return ECONNABORTED;
	case WSAECONNRESET:
		return ECONNRESET;
	case WSAECONNREFUSED:
		return ECONNREFUSED;
	case WSAENETDOWN:
		return ENETDOWN;
	case WSAENETUNREACH:
		return ENETUNREACH;
	case WSAEHOSTUNREACH:
		return EHOSTUNREACH;
	case WSAETIMEDOUT:
		return ETIMEDOUT;
	case WSAENOTCONN:
		return ENOTCONN;
	case WSAESHUTDOWN:
		return EPIPE;
	case WSAEMSGSIZE:
		return EMSGSIZE;
	case WSAENOBUFS:
		return ENOBUFS;
	default:
		return EIO;
	}
}

/* Synchronous initiation APIs still report errno to their caller. */
static inline int async_win_error_to_errno(int value)
{
	return async_error_to_errno(async_native_error(static_cast<DWORD>(value)));
}

#endif /* _ASYNC_ERROR_H_ */
