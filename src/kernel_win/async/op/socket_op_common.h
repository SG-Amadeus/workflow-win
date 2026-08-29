/* Common Windows error mapping used by socket operation completions. */
#ifndef _ASYNC_OP_SOCKET_OP_COMMON_H_
#define _ASYNC_OP_SOCKET_OP_COMMON_H_

#include <WinSock2.h>
#include <Windows.h>
#include <errno.h>

#include "../service/cancel_token.h"
#include "../error.h"

static inline async_error_code socket_op_normalize_error(
	async_error_code error, const cancel_token *cancel_token)
{
	if (error.value() == ERROR_OPERATION_ABORTED)
		return async_system_error(ERROR_OPERATION_ABORTED);

	if (error.value() == ERROR_NETNAME_DELETED)
	{
		if (cancel_token && cancel_token->is_closed())
			return async_system_error(ERROR_OPERATION_ABORTED);
		return async_socket_error(WSAECONNRESET);
	}

	if (error.value() == ERROR_PORT_UNREACHABLE ||
		error.value() == ERROR_CONNECTION_REFUSED)
		return async_socket_error(WSAECONNREFUSED);

	if (error.value() == ERROR_NETWORK_UNREACHABLE)
		return async_socket_error(WSAENETUNREACH);

	if (error.category() == std::system_category())
		return error;

	/* A failed WSA call is delivered by IOCP as its native WSA value.  ASIO
	 * exposes that value through the socket category, so classify it here
	 * before the operation reaches its handler. */
	switch (error.value())
	{
	case WSAEINTR:
	case WSAEBADF:
	case WSAEACCES:
	case WSAEFAULT:
	case WSAEINVAL:
	case WSAEMFILE:
	case WSAEWOULDBLOCK:
	case WSAEINPROGRESS:
	case WSAEALREADY:
	case WSAENOTSOCK:
	case WSAEDESTADDRREQ:
	case WSAEMSGSIZE:
	case WSAEPROTOTYPE:
	case WSAENOPROTOOPT:
	case WSAEPROTONOSUPPORT:
	case WSAESOCKTNOSUPPORT:
	case WSAEOPNOTSUPP:
	case WSAEPFNOSUPPORT:
	case WSAEAFNOSUPPORT:
	case WSAEADDRINUSE:
	case WSAEADDRNOTAVAIL:
	case WSAENETDOWN:
	case WSAENETUNREACH:
	case WSAENETRESET:
	case WSAECONNABORTED:
	case WSAECONNRESET:
	case WSAENOBUFS:
	case WSAEISCONN:
	case WSAENOTCONN:
	case WSAESHUTDOWN:
	case WSAETIMEDOUT:
	case WSAECONNREFUSED:
	case WSAEHOSTUNREACH:
		return async_socket_error(error.value());
	default:
		break;
	}

	return async_native_error(static_cast<DWORD>(error.value()));
}

#endif /* _ASYNC_OP_SOCKET_OP_COMMON_H_ */
