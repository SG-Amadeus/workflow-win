#ifndef _ASYNC_SSL_STREAM_IMPL_H_
#define _ASYNC_SSL_STREAM_IMPL_H_

#include "ssl_stream.h"
#include "tcp_socket.h"

#include <openssl/ssl.h>
#include <openssl/err.h>

#include "../list.h"
class ssl_op;
class ssl_stream::impl
{
public:
	executor executor_;
	SSL_CTX *ctx_;
	SSL *ssl_;
	BIO *ext_bio_;
	tcp_socket socket_;
	int server_;
	ssl_op *control_current_;
	ssl_op *read_current_;
	ssl_op *write_current_;
	struct list_head control_waiting_;
	struct list_head read_waiting_;
	struct list_head write_waiting_;
	volatile LONG closed_;
	CRITICAL_SECTION mutex_;
	CRITICAL_SECTION engine_lock_;
	ssl_stream_init_callback init_cb_;
	void *init_userdata_;
	volatile LONG refs_;
	int init_error_;
	char input_buffer_[17 * 1024];
	size_t input_size_;

	impl(executor ex, SSL_CTX *ctx, int server)
		: executor_(ex),
		  ctx_(ctx),
		  ssl_(nullptr),
		  ext_bio_(nullptr),
		  socket_(),
		  server_(server != 0),
		  control_current_(nullptr),
		  read_current_(nullptr),
		  write_current_(nullptr),
		  closed_(0),
			 init_cb_(nullptr),
			 init_userdata_(nullptr),
			 init_error_(0),
			 input_size_(0)
	{
		::InitializeCriticalSection(&mutex_);
		::InitializeCriticalSection(&engine_lock_);
		refs_ = 1;
		INIT_LIST_HEAD(&control_waiting_);
		INIT_LIST_HEAD(&read_waiting_);
		INIT_LIST_HEAD(&write_waiting_);

		if (ctx_ && SSL_CTX_up_ref(ctx_) != 1)
		{
			ctx_ = nullptr;
			init_error_ = ENOMEM;
		}
		if (socket_.init(ex) != 0)
			init_error_ = ENOMEM;
		else if (!ctx_ && !init_error_)
			init_error_ = EINVAL;

		if (!init_error_)
		{
			ssl_ = SSL_new(ctx_);
			if (!ssl_)
				init_error_ = ENOMEM;
		}

		if (ssl_)
		{
			BIO *int_bio = nullptr;
			if (BIO_new_bio_pair(&int_bio, 17 * 1024,
								 &ext_bio_, 17 * 1024) != 1)
			{
				SSL_free(ssl_);
				ssl_ = nullptr;
				ext_bio_ = nullptr;
				init_error_ = ENOMEM;
			}
			else
			{
				SSL_set_bio(ssl_, int_bio, int_bio);
				SSL_set_mode(ssl_, SSL_MODE_ENABLE_PARTIAL_WRITE);
				SSL_set_mode(ssl_, SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
#if defined(SSL_MODE_RELEASE_BUFFERS)
				SSL_set_mode(ssl_, SSL_MODE_RELEASE_BUFFERS);
#endif
			}
		}
	}

	~impl()
	{
		if (ext_bio_)
			BIO_free(ext_bio_);
		if (ssl_)
			SSL_free(ssl_);
		if (ctx_)
			SSL_CTX_free(ctx_);
		::DeleteCriticalSection(&mutex_);
		::DeleteCriticalSection(&engine_lock_);
	}

	void acquire()
	{
		::InterlockedIncrement(&refs_);
	}

	void release()
	{
		if (::InterlockedDecrement(&refs_) == 0)
		{
			this->~impl();
			free(this);
		}
	}
};

#endif /* _ASYNC_SSL_STREAM_IMPL_H_ */


