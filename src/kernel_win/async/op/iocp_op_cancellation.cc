#include "iocp_op_cancellation.h"

#include <errno.h>
#include <new>
#include <stdlib.h>

iocp_op_cancellation::iocp_op_cancellation(
	HANDLE handle, win_iocp_operation *target,
	const cancellation_slot &slot, volatile LONG *cancel_requested)
	: win_iocp_operation(&iocp_op_cancellation::do_complete),
	  handle_(handle), target_(target), slot_(slot),
	  cancel_requested_(cancel_requested), refs_(1)
{
	slot_.assign(&iocp_op_cancellation::do_cancel, this,
		&iocp_op_cancellation::acquire, &iocp_op_cancellation::release);
}

iocp_op_cancellation::~iocp_op_cancellation()
{
	slot_.clear(&iocp_op_cancellation::do_cancel, this);
}

iocp_op_cancellation *iocp_op_cancellation::create(
	HANDLE handle, win_iocp_operation *target,
	const cancellation_slot &slot, volatile LONG *cancel_requested)
{
	if (!slot.is_connected())
		return nullptr;

	void *mem = malloc(sizeof(iocp_op_cancellation));
	if (!mem)
	{
		errno = ENOMEM;
		return nullptr;
	}

	return new (mem) iocp_op_cancellation(handle, target, slot,
										 cancel_requested);
}

void iocp_op_cancellation::destroy(iocp_op_cancellation *op)
{
	if (op)
		iocp_op_cancellation::release(op);
}

void iocp_op_cancellation::acquire(void *context)
{
	iocp_op_cancellation *self =
		static_cast<iocp_op_cancellation *>(context);
	::InterlockedIncrement(&self->refs_);
}

void iocp_op_cancellation::release(void *context)
{
	iocp_op_cancellation *self =
		static_cast<iocp_op_cancellation *>(context);
	if (::InterlockedDecrement(&self->refs_) == 0)
	{
		self->~iocp_op_cancellation();
		free(self);
	}
}

void iocp_op_cancellation::do_cancel(void *context, cancellation_type type)
{
	if (type == cancellation_type::none)
		return;

	iocp_op_cancellation *self =
		static_cast<iocp_op_cancellation *>(context);
	if (self->cancel_requested_)
		::InterlockedExchange(self->cancel_requested_, 1);
	::CancelIoEx(self->handle_, self);
}

void iocp_op_cancellation::do_complete(void *owner,
	win_iocp_operation *base, async_error_code error, size_t bytes)
{
	iocp_op_cancellation *self =
		static_cast<iocp_op_cancellation *>(base);
	win_iocp_operation *target = self->target_;
	self->target_ = nullptr;
	self->slot_.clear(&iocp_op_cancellation::do_cancel, self);
	/* Release the IOCP ownership after removing the registration.  A concurrent
	 * signal emit holds the extra handler reference until CancelIoEx returns. */
	destroy(self);
	target->complete(owner, error, bytes);
}
