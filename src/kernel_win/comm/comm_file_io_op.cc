#include "comm_file_io_op.h"

#include <assert.h>
#include <errno.h>
#include <stdlib.h>

#include <new>

#include "../async/random_access_handle.h"

comm_file_io_op::comm_file_io_op()
	: ctx_(nullptr), session_(nullptr), service_(nullptr),
	  context_released_(0), service_released_(0)
{
	composed_op_init(this, &comm_file_io_op::destroy,
					 &comm_file_io_op::complete);
}

void comm_file_io_op::release_context_data(FileIOContext *fc,
									 bool release_service)
{
	if (!fc)
		return;

	CommunicatorImpl *impl = fc->impl;
	IOSession *session = fc->session;
	IOService *service = session ? session->service : nullptr;
	impl->unregister_file(fc);
	impl->release_file_handle(fc->handle);
	fc->~FileIOContext();
	free(fc);
	if (release_service && service)
		service->release_session(session);
}

void comm_file_io_op::release_context(comm_file_io_op *self)
{
	if (::InterlockedExchange(&self->context_released_, 1) != 0)
		return;

	FileIOContext *fc = self->ctx_;
	self->ctx_ = nullptr;
	if (!fc)
		return;

	self->session_ = fc->session;
	self->service_ = self->session_ ? self->session_->service : nullptr;
	comm_file_io_op::release_context_data(fc, false);
}

void comm_file_io_op::release_service(comm_file_io_op *self)
{
	if (::InterlockedExchange(&self->service_released_, 1) != 0)
		return;

	if (self->service_)
		self->service_->release_session(self->session_);
}

void comm_file_io_op::destroy(composed_op *base)
{
	comm_file_io_op *self = static_cast<comm_file_io_op *>(base);
	/* The ASIO handler may be destroyed without invocation during context
	 * shutdown.  Release the Workflow request context, but do not synthesize
	 * a business callback from the handler destructor. */
	comm_file_io_op::release_context(self);
	comm_file_io_op::release_service(self);
	self->~comm_file_io_op();
	free(self);
}

void comm_file_io_op::complete(composed_op *base)
{
	comm_file_io_op *self = static_cast<comm_file_io_op *>(base);
	IOSession *session = self->session_;
	if (session)
	{
		int error = async_error_to_errno(self->result_error_);
		if (error == 0)
			session->res = self->result_bytes_;
		session->handle(error == 0 ? IOS_STATE_SUCCESS : IOS_STATE_ERROR,
						error);
	}
	/* IOService::nevents covers the business callback as well as the I/O. */
	comm_file_io_op::release_service(self);
	self->session_ = nullptr;
	self->service_ = nullptr;
}

void comm_file_io_op::file_cb(void *ctx, async_error_code error, size_t bytes)
{
	comm_file_io_op *self = static_cast<comm_file_io_op *>(ctx);
	comm_file_io_op::post_completion(self, error, bytes);
	composed_op_release(self);
}

void comm_file_io_op::file_destroy(void *ctx)
{
	comm_file_io_op *self = static_cast<comm_file_io_op *>(ctx);
	comm_file_io_op::release_context(self);
	comm_file_io_op::release_service(self);
	composed_op_release(self);
}

void comm_file_io_op::handle_complete(void *ctx)
{
	comm_file_io_op *self = static_cast<comm_file_io_op *>(ctx);
	composed_op_complete_handler(self);
	composed_op_release(self);
}

void comm_file_io_op::post_completion(comm_file_io_op *self,
								  async_error_code error, size_t bytes)
{
	if (!composed_op_try_complete(self))
		return;
	self->session_ = self->ctx_ ? self->ctx_->session : nullptr;
	self->result_error_ = error;
	self->result_bytes_ = bytes;
	CommunicatorImpl *impl = self->ctx_ ? self->ctx_->impl : nullptr;
	comm_file_io_op::release_context(self);
	/* The IOCP phase has retired; the remaining reference belongs to the
	 * Workflow handler pool and must not keep the IO context alive. */
	self->work_.reset();
	::InterlockedExchange(&self->result_ready_, 1);
	::InterlockedExchange(&self->dispatch_started_, 1);
	composed_op_add_ref(self);
	int ret = impl ? impl->post_handler(&comm_file_io_op::handle_complete,
			self, &self->handler_task_) : -1;
	/* An accepted operation cannot outlive the handler pool.  Never execute a
	 * Workflow callback on an IOCP worker when this invariant is violated. */
	assert(ret == 0);
	if (ret != 0)
		RaiseFailFastException(nullptr, nullptr, 0);
}

int comm_file_io_op::start(FileIOContext *ctx)
{
	if (!ctx || !ctx->impl || !ctx->session || !ctx->handle ||
		!ctx->session->file)
	{
		errno = EINVAL;
		return -1;
	}

	void *mem = malloc(sizeof(comm_file_io_op));
	if (!mem)
	{
		comm_file_io_op::release_context_data(ctx, true);
		errno = ENOMEM;
		return -1;
	}

	comm_file_io_op *op = new (mem) comm_file_io_op();
	op->ctx_ = ctx;
	composed_op_set_executor(op, ctx->handle->file.get_executor());
	composed_op_add_ref(op);

	IOSession *session = ctx->session;
	int ret;
	if (session->operation == IOSession::OP_PREAD)
		ret = ctx->handle->file.async_read_some_at((uint64_t)session->offset,
			session->buf, session->count, &comm_file_io_op::file_cb, op,
			&comm_file_io_op::file_destroy);
	else if (session->operation == IOSession::OP_PWRITE)
		ret = ctx->handle->file.async_write_some_at((uint64_t)session->offset,
			session->buf, session->count, &comm_file_io_op::file_cb, op,
			&comm_file_io_op::file_destroy);
	else if (session->operation == IOSession::OP_PREADV)
		ret = ctx->handle->file.async_readv_at((uint64_t)session->offset,
			session->iov, session->iovcnt, &comm_file_io_op::file_cb, op,
			&comm_file_io_op::file_destroy);
	else if (session->operation == IOSession::OP_PWRITEV)
		ret = ctx->handle->file.async_writev_at((uint64_t)session->offset,
			session->iov, session->iovcnt, &comm_file_io_op::file_cb, op,
			&comm_file_io_op::file_destroy);
	else if (session->operation == IOSession::OP_FSYNC ||
			 session->operation == IOSession::OP_FDSYNC)
		ret = ctx->handle->file.async_fsync(&comm_file_io_op::file_cb, op,
			&comm_file_io_op::file_destroy);
	else
	{
		composed_op_release(op);
		composed_op_release(op);
		errno = ENOSYS;
		return -1;
	}

	if (ret < 0)
	{
		int error = errno;
		composed_op_release(op);
		composed_op_release(op);
		errno = error;
		return -1;
	}

	composed_op_release(op);
	return 0;
}

int comm_file_io_op::start(CommunicatorImpl *impl, IOSession *session)
{
	if (!impl || !session || !session->file)
	{
		errno = EINVAL;
		return -1;
	}

	FileHandleContext *handle = impl->acquire_file_handle(session->file);
	if (!handle)
		return -1;

	void *mem = malloc(sizeof(FileIOContext));
	if (!mem)
	{
		impl->release_file_handle(handle);
		errno = ENOMEM;
		return -1;
	}

	FileIOContext *context = new (mem) FileIOContext(impl, session, handle);

	if (!impl->register_file(context))
	{
		impl->release_file_handle(handle);
		context->~FileIOContext();
		free(context);
		errno = ECANCELED;
		return -1;
	}

	if (start(context) == 0)
		return 0;

	int error = errno ? errno : EIO;
	errno = error;
	return -1;
}

