/*
  Copyright (c) 2019 Sogou, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

  Authors: Xie Han (xiehan@sogou-inc.com)
           Wu Jiaxu (wujiaxu@sogou-inc.com)
*/

#include <errno.h>
#include <stdio.h>
#include <atomic>
#include <openssl/ssl.h>
#include "PlatformSocket.h"
#include "CommScheduler.h"
#include "WFConnection.h"
#include "WFGlobal.h"
#include "WFServer.h"

#ifdef _WIN32
#include <io.h>
/* PlatformSocket.h already pulled in WinSock2; Windows.h only adds the
 * process/wait facilities used by serve(SOCKET). */
#include <Windows.h>
#else
#include <unistd.h>
#endif

#define PORT_STR_MAX	5

class WFServerConnection : public WFConnection
{
public:
	WFServerConnection(std::atomic<size_t> *conn_count)
	{
		this->conn_count = conn_count;
	}

	virtual ~WFServerConnection()
	{
		(*this->conn_count)--;
	}

private:
	std::atomic<size_t> *conn_count;
};

int WFServerBase::ssl_ctx_callback(SSL *ssl, int *al, void *arg)
{
	WFServerBase *server = (WFServerBase *)arg;
	const char *servername = SSL_get_servername(ssl, TLSEXT_NAMETYPE_host_name);
	SSL_CTX *ssl_ctx = server->get_server_ssl_ctx(servername);

	if (!ssl_ctx)
		return SSL_TLSEXT_ERR_NOACK;

	if (ssl_ctx != server->get_ssl_ctx())
		SSL_set_SSL_CTX(ssl, ssl_ctx);

	return SSL_TLSEXT_ERR_OK;
}

SSL_CTX *WFServerBase::new_ssl_ctx(const char *cert_file, const char *key_file)
{
	SSL_CTX *ssl_ctx = WFGlobal::new_ssl_server_ctx();

	if (!ssl_ctx)
		return NULL;

	if (SSL_CTX_use_certificate_chain_file(ssl_ctx, cert_file) > 0 &&
		SSL_CTX_use_PrivateKey_file(ssl_ctx, key_file, SSL_FILETYPE_PEM) > 0 &&
		SSL_CTX_check_private_key(ssl_ctx) > 0 &&
		SSL_CTX_set_tlsext_servername_callback(ssl_ctx, ssl_ctx_callback) > 0 &&
		SSL_CTX_set_tlsext_servername_arg(ssl_ctx, this) > 0)
	{
		return ssl_ctx;
	}

	SSL_CTX_free(ssl_ctx);
	return NULL;
}

int WFServerBase::init(const struct sockaddr *bind_addr, socklen_t addrlen,
					   const char *cert_file, const char *key_file)
{
	int timeout = this->params.peer_response_timeout;

	if (this->params.receive_timeout >= 0)
	{
		if ((unsigned int)timeout > (unsigned int)this->params.receive_timeout)
			timeout = this->params.receive_timeout;
	}

#ifdef _WIN32
	/* The Windows kernel has no SCTP path; reject it up front. */
	if (this->params.transport_type == TT_SCTP ||
		this->params.transport_type == TT_SCTP_SSL)
	{
		errno = EPROTONOSUPPORT;
		return -1;
	}
#endif

	if (this->CommService::init(bind_addr, addrlen, -1, timeout) < 0)
		return -1;

#ifdef _WIN32
	/* CommService::init() resets reliable to 1 (stream); a datagram
	 * service must opt out after init and before bind(). */
	if (this->params.transport_type == TT_UDP)
		this->set_reliable(0);
#endif

	if (key_file && cert_file && this->params.transport_type != TT_UDP)
	{
		SSL_CTX *ssl_ctx = this->new_ssl_ctx(cert_file, key_file);

		if (!ssl_ctx)
		{
			this->deinit();
			return -1;
		}

		this->set_ssl(ssl_ctx, this->params.ssl_accept_timeout);
	}

	this->scheduler = WFGlobal::get_scheduler();
	return 0;
}

#ifndef _WIN32
int WFServerBase::create_listen_fd()
{
	if (this->listen_fd < 0)
	{
		const struct sockaddr *bind_addr;
		socklen_t addrlen;
		int reuse = 1;

		this->get_addr(&bind_addr, &addrlen);
		this->listen_fd = (int)socket(bind_addr->sa_family, SOCK_STREAM, 0);
		if (this->listen_fd >= 0)
		{
			setsockopt(this->listen_fd, SOL_SOCKET, SO_REUSEADDR,
					   (const char *)(&reuse), sizeof (int));
		}
	}
	else
		this->listen_fd = dup(this->listen_fd);

	return this->listen_fd;
}

#else

SOCKET WFServerBase::create_listen_socket()
{
	WSAPROTOCOL_INFOW protocol_info;
	const struct sockaddr *bind_addr;
	socklen_t addrlen;
	SOCKET duplicate;
	SOCKET pending;

	/* serve(SOCKET) borrowed the caller's descriptor only for the
	 * synchronous serve() -> bind() -> create_listen_socket() stack
	 * (01 7.1): the Service takes an independent duplicate and never
	 * closes the caller's original.  Any other entry point creates a
	 * fresh socket mirroring CommService::create_listen_socket().
	 * pending_listen_socket is restored to INVALID_SOCKET before every
	 * return here. */
	pending = this->pending_listen_socket;
	if (pending == INVALID_SOCKET)
	{
		this->get_addr(&bind_addr, &addrlen);
		return WSASocketW(bind_addr->sa_family, SOCK_STREAM, IPPROTO_TCP,
						  NULL, 0, WSA_FLAG_OVERLAPPED);
	}

	this->pending_listen_socket = INVALID_SOCKET;
	if (WSADuplicateSocketW(pending, GetCurrentProcessId(),
							&protocol_info) < 0)
		return INVALID_SOCKET;

	duplicate = WSASocketW(FROM_PROTOCOL_INFO, FROM_PROTOCOL_INFO,
						   FROM_PROTOCOL_INFO, &protocol_info, 0,
						   WSA_FLAG_OVERLAPPED);
	return duplicate;
}

#endif

#ifdef _WIN32
WFConnection *WFServerBase::new_connection(SOCKET accept_socket)
#else
WFConnection *WFServerBase::new_connection(int accept_fd)
#endif
{
	if (++this->conn_count > this->params.max_connections &&
		this->drain(1) <= 0)
	{
		this->conn_count--;
		errno = EMFILE;
		return NULL;
	}

	return new WFServerConnection(&this->conn_count);
}

void WFServerBase::delete_connection(WFConnection *conn)
{
	delete (WFServerConnection *)conn;
}

void WFServerBase::handle_unbound()
{
	this->mutex.lock();
	this->unbind_finish = true;
	this->cond.notify_one();
	this->mutex.unlock();
}

int WFServerBase::start(const struct sockaddr *bind_addr, socklen_t addrlen,
						const char *cert_file, const char *key_file)
{
	SSL_CTX *ssl_ctx;

	if (this->init(bind_addr, addrlen, cert_file, key_file) >= 0)
	{
		if (this->scheduler->bind(this) >= 0)
			return 0;

		ssl_ctx = this->get_ssl_ctx();
		this->deinit();
		if (ssl_ctx)
			SSL_CTX_free(ssl_ctx);
	}

#ifndef _WIN32
	this->listen_fd = -1;
#endif
	return -1;
}

int WFServerBase::start(int family, const char *host, unsigned short port,
						const char *cert_file, const char *key_file)
{
	struct addrinfo hints;
	struct addrinfo *addrinfo;
	char port_str[PORT_STR_MAX + 1];
	int ret;

	memset(&hints, 0, sizeof (hints));
	hints.ai_flags = AI_PASSIVE;
	hints.ai_family = family;
	hints.ai_socktype = SOCK_STREAM;
	snprintf(port_str, PORT_STR_MAX + 1, "%d", port);
	ret = getaddrinfo(host, port_str, &hints, &addrinfo);
	if (ret == 0)
	{
		ret = start(addrinfo->ai_addr, (socklen_t)addrinfo->ai_addrlen,
					cert_file, key_file);
		freeaddrinfo(addrinfo);
	}
	else
	{
#ifdef EAI_SYSTEM
		if (ret != EAI_SYSTEM)
			errno = EINVAL;
#endif
		ret = -1;
	}

	return ret;
}

#ifdef _WIN32
int WFServerBase::serve(SOCKET listen_socket,
						const char *cert_file, const char *key_file)
{
	struct sockaddr_storage ss;
	socklen_t len = sizeof ss;
	int ret;

	if (getsockname(listen_socket, (struct sockaddr *)&ss, &len) < 0)
		return -1;

	/* The caller keeps owning the descriptor (01 7.1); it is only
	 * borrowed for the synchronous stack and never stored long-term. */
	this->pending_listen_socket = listen_socket;
	ret = start((struct sockaddr *)&ss, len, cert_file, key_file);
	if (this->pending_listen_socket != INVALID_SOCKET)
		this->pending_listen_socket = INVALID_SOCKET;

	return ret;
}
#else
int WFServerBase::serve(int listen_fd,
						const char *cert_file, const char *key_file)
{
	struct sockaddr_storage ss;
	socklen_t len = sizeof ss;

	if (getsockname(listen_fd, (struct sockaddr *)&ss, &len) < 0)
		return -1;

	this->listen_fd = listen_fd;
	return start((struct sockaddr *)&ss, len, cert_file, key_file);
}
#endif

void WFServerBase::shutdown()
{
#ifndef _WIN32
	this->listen_fd = -1;
#endif
	this->scheduler->unbind(this);
}

void WFServerBase::wait_finish()
{
	SSL_CTX *ssl_ctx = this->get_ssl_ctx();
	std::unique_lock<std::mutex> lock(this->mutex);

	while (!this->unbind_finish)
		this->cond.wait(lock);

	this->deinit();
	this->unbind_finish = false;
	lock.unlock();
	if (ssl_ctx)
		SSL_CTX_free(ssl_ctx);
}

#ifdef _WIN32
int WFServerBase::get_listen_addr(struct sockaddr *addr, socklen_t *addrlen) const
{
	const struct sockaddr *bind_addr;
	socklen_t len;

	/* The Service keeps the actual bound address (bind() refreshed it via
	 * getsockname, 01 7.1), so no live socket needs to be stored here. */
	this->get_addr(&bind_addr, &len);
	if (!bind_addr)
	{
		errno = ENOTCONN;
		return -1;
	}

	if (*addrlen < len)
		len = *addrlen;
	memcpy(addr, bind_addr, len);
	*addrlen = len;
	return 0;
}
#endif

