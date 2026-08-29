#include "composed_op.h"
#include "async_handler.h"

namespace
{

void composed_op_acquire(void *context)
{
	composed_op_add_ref(static_cast<composed_op *>(context));
}

void composed_op_release_owner(void *context)
{
	composed_op_release(static_cast<composed_op *>(context));
}

void composed_op_cancel_notify(void *context, cancellation_type type)
{
	composed_op *op = static_cast<composed_op *>(context);
	if (::InterlockedExchangeAdd(&op->completed_, 0) != 0)
		return;
	if (op->cancel_)
		op->cancel_(op, type);
}

void composed_op_complete_handler(void *context, async_error_code /*error*/,
								 size_t /*bytes*/)
{
	composed_op *op = static_cast<composed_op *>(context);
	op->work_.reset();
	op->complete_(op);
	composed_op_release(op);
}

int composed_op_start_dispatch(composed_op *op)
{
	if (::InterlockedExchangeAdd(&op->refs_, 0) != 0 ||
		::InterlockedExchangeAdd(&op->result_ready_, 0) == 0 ||
		::InterlockedExchangeAdd(&op->abandoned_, 0) != 0 ||
		::InterlockedCompareExchange(&op->dispatch_started_, 1, 0) != 0)
		return 0;

	/* Keep the state alive while the final handler is queued or running. */
	::InterlockedIncrement(&op->refs_);
	async_handler handler = {
		&composed_op_complete_handler, op, &composed_op_destroy
	};
	(void)async_handler_dispatch(op->executor_, handler, async_error_code(), 0);
	return 0;
}

} /* namespace */

void composed_op_init(composed_op *op, void (*destroy)(composed_op *),
					  void (*complete)(composed_op *))
{
	op->refs_ = 1;
	op->completed_ = 0;
	op->result_ready_ = 0;
	op->dispatch_started_ = 0;
	op->abandoned_ = 0;
	op->invocations_ = 0;
	op->destroy_ = destroy;
	op->complete_ = complete;
	op->step_ = nullptr;
	op->cancel_ = nullptr;
	op->ctx_destroy_ = nullptr;
	op->ctx_ = nullptr;
	op->cancel_state_.set_owner(op, &composed_op_acquire,
								 &composed_op_release_owner);
}

void composed_op_set_step(composed_op *op, composed_op::step_type step)
{
	op->step_ = step;
}

void composed_op_start(composed_op *op)
{
	composed_op_invoke(op, async_error_code(), 0);
}

void composed_op_invoke(composed_op *op, async_error_code error, size_t bytes)
{
	if (!op || !op->step_ ||
		::InterlockedExchangeAdd(&op->abandoned_, 0) != 0)
		return;

	if (::InterlockedExchangeAdd(&op->invocations_, 0) != LONG_MAX)
		::InterlockedIncrement(&op->invocations_);
	/* ASIO clears the intermediate cancellation slot before each continuation
	 * invocation.  The next child operation installs its own cancellation
	 * handler through the same state. */
	op->cancel_state_.slot().clear();
	op->step_(op, error, bytes);
}

void composed_op_complete(composed_op *op, async_error_code error, size_t bytes)
{
	if (!op || !composed_op_try_complete(op))
		return;

	op->result_error_ = error;
	op->result_bytes_ = bytes;
	::InterlockedExchange(&op->result_ready_, 1);
	composed_op_start_dispatch(op);
}

void composed_op_complete_handler(composed_op *op)
{
	if (!op || !op->complete_)
		return;
	op->work_.reset();
	op->complete_(op);
}

void composed_op_reset_cancellation(composed_op *op)
{
	if (op)
		op->cancel_state_.clear();
}

cancellation_type composed_op_cancelled(const composed_op *op)
{
	return op ? op->cancel_state_.cancelled() : cancellation_type::none;
}

void composed_op_set_executor(composed_op *op, executor ex)
{
	op->executor_ = ex;
	op->work_.set_executor(ex);
}

void composed_op_set_cancellation(composed_op *op,
							  const cancellation_slot &slot,
							  void (*cancel)(composed_op *, cancellation_type))
{
	op->cancel_ = cancel;
	op->cancel_state_.connect(slot);
	op->cancel_state_.set_notify(composed_op_cancel_notify, op);
}

cancellation_slot composed_op_cancellation_slot(composed_op *op)
{
	return op->cancel_state_.slot();
}

void composed_op_set_ctx(composed_op *op, void (*destroy)(void *), void *ctx)
{
	op->ctx_destroy_ = destroy;
	op->ctx_ = ctx;
}

void composed_op_add_ref(composed_op *op)
{
	::InterlockedIncrement(&op->refs_);
}

void composed_op_release(composed_op *op)
{
	LONG refs = ::InterlockedDecrement(&op->refs_);
	if (refs != 0)
		return;

	/* ASIO's composed handler is destroyed only after all child handlers have
	 * retired.  If a child selected a result, turn the last child reference
	 * into the final-handler reference before destroying the state. */
	if (::InterlockedExchangeAdd(&op->result_ready_, 0) != 0 &&
		::InterlockedExchangeAdd(&op->abandoned_, 0) == 0 &&
		::InterlockedCompareExchange(&op->dispatch_started_, 1, 0) == 0)
	{
		::InterlockedIncrement(&op->refs_);
		async_handler handler = {
			&composed_op_complete_handler, op, &composed_op_destroy
		};
		(void)async_handler_dispatch(op->executor_, handler, async_error_code(), 0);
		return;
	}

	/* The user context outlives the user callback.  If a child was abandoned,
	 * this is handler destruction only; otherwise it follows the final
	 * completion handler's retirement. */
	if (op->ctx_destroy_)
	{
		void (*destroy)(void *) = op->ctx_destroy_;
		void *ctx = op->ctx_;
		op->ctx_destroy_ = nullptr;
		op->ctx_ = nullptr;
		destroy(ctx);
	}
	op->destroy_(op);
}

void composed_op_abandon(composed_op *op)
{
	if (!op)
		return;
	::InterlockedExchange(&op->abandoned_, 1);
	composed_op_release(op);
}

int composed_op_try_complete(composed_op *op)
{
	return ::InterlockedCompareExchange(&op->completed_, 1, 0) == 0;
}

void composed_op_complete_with_error(composed_op *op, async_error_code error,
								 size_t bytes)
{
	if (!composed_op_try_complete(op))
		return;

	op->result_error_ = error;
	op->result_bytes_ = bytes;
	::InterlockedExchange(&op->result_ready_, 1);
	if (op->cancel_)
		op->cancel_(op, cancellation_type::terminal);
	composed_op_start_dispatch(op);
}

int composed_op_dispatch_result(composed_op *op, async_error_code error,
								 size_t bytes)
{
	op->result_error_ = error;
	op->result_bytes_ = bytes;
	::InterlockedExchange(&op->result_ready_, 1);
	return composed_op_start_dispatch(op);
}

int composed_op_dispatch(composed_op *op)
{
	/* The non-payload form is used by composed operations that have already
	 * stored their result fields.  It follows the same last-child rule. */
	::InterlockedExchange(&op->result_ready_, 1);
	return composed_op_start_dispatch(op);
}

void composed_op_cancel(composed_op *op, cancellation_type type)
{
	if (!op || type == cancellation_type::none)
		return;
	op->cancel_state_.slot().emit(type);
}

void composed_op_destroy(void *context)
{
	composed_op_abandon(static_cast<composed_op *>(context));
}

void composed_op_release_context(void *context)
{
	composed_op_release(static_cast<composed_op *>(context));
}
