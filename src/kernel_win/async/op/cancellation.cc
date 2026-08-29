#include "cancellation.h"

cancellation_slot::cancellation_slot()
	: signal_(nullptr), owner_(nullptr), owner_acquire_(nullptr),
	  owner_release_(nullptr)
{
}

cancellation_slot::cancellation_slot(const cancellation_slot &other)
	: signal_(other.signal_), owner_(other.owner_),
	  owner_acquire_(other.owner_acquire_), owner_release_(other.owner_release_)
{
	if (owner_ && owner_acquire_)
		owner_acquire_(owner_);
}

cancellation_slot::cancellation_slot(cancellation_signal *signal,
								 void *owner,
								 cancellation_slot_ref acquire,
								 cancellation_slot_ref release)
	: signal_(signal), owner_(owner), owner_acquire_(acquire),
	  owner_release_(release)
{
	if (owner_ && owner_acquire_)
		owner_acquire_(owner_);
}

cancellation_slot &cancellation_slot::operator=(const cancellation_slot &other)
{
	if (this == &other)
		return *this;
	if (owner_ && owner_release_)
		owner_release_(owner_);
	signal_ = other.signal_;
	owner_ = other.owner_;
	owner_acquire_ = other.owner_acquire_;
	owner_release_ = other.owner_release_;
	if (owner_ && owner_acquire_)
		owner_acquire_(owner_);
	return *this;
}

cancellation_slot::~cancellation_slot()
{
	if (owner_ && owner_release_)
		owner_release_(owner_);
}

bool cancellation_slot::is_connected() const
{
	return signal_ != nullptr;
}

void cancellation_slot::clear()
{
	if (signal_)
		signal_->clear();
}

void cancellation_slot::clear(cancellation_handler handler, void *context)
{
	if (signal_)
		signal_->clear(handler, context);
}

void cancellation_slot::assign(cancellation_handler handler, void *context)
{
	if (signal_)
		signal_->assign(handler, context);
}

void cancellation_slot::assign(cancellation_handler handler, void *context,
								cancellation_handler_ref acquire,
								cancellation_handler_ref release)
{
	if (signal_)
		signal_->assign(handler, context, acquire, release);
}

void cancellation_slot::emit(cancellation_type type) const
{
	if (signal_)
		signal_->emit(type);
}

cancellation_signal::cancellation_signal()
	: handler_{nullptr, nullptr}, handler_acquire_(nullptr),
	  handler_release_(nullptr), owner_(nullptr), owner_acquire_(nullptr),
	  owner_release_(nullptr)
{
	::InitializeCriticalSection(&lock_);
}

cancellation_signal::~cancellation_signal()
{
	cancellation_handler_ref release;
	void *context;
	::EnterCriticalSection(&lock_);
	handler_.fn = nullptr;
	context = handler_.context;
	release = handler_release_;
	handler_.context = nullptr;
	handler_acquire_ = nullptr;
	handler_release_ = nullptr;
	::LeaveCriticalSection(&lock_);
	if (release)
		release(context);
	::DeleteCriticalSection(&lock_);
}

cancellation_slot cancellation_signal::slot() const
{
	return cancellation_slot(const_cast<cancellation_signal *>(this), owner_,
							 owner_acquire_, owner_release_);
}

void cancellation_signal::emit(cancellation_type type)
{
	if (type == cancellation_type::none)
		return;

	/* Pin the current handler while it is being called, but do not call it
	 * while holding the signal lock.  CancelIoEx may cause the overlapped
	 * completion path to destroy the slot. */
	cancellation_handler handler;
	void *context;
	cancellation_handler_ref acquire;
	cancellation_handler_ref release;
	::EnterCriticalSection(&lock_);
	handler = handler_.fn;
	context = handler_.context;
	acquire = handler_acquire_;
	release = handler_release_;
	if (handler && acquire)
		acquire(context);
	::LeaveCriticalSection(&lock_);

	if (handler)
		handler(context, type);

	if (handler && release)
		release(context);
}

void cancellation_signal::clear()
{
	cancellation_handler_ref release;
	void *context;
	::EnterCriticalSection(&lock_);
	handler_.fn = nullptr;
	context = handler_.context;
	handler_.context = nullptr;
	release = handler_release_;
	handler_acquire_ = nullptr;
	handler_release_ = nullptr;
	::LeaveCriticalSection(&lock_);
	if (release)
		release(context);
}

void cancellation_signal::clear(cancellation_handler handler, void *context)
{
	cancellation_handler_ref release = nullptr;
	::EnterCriticalSection(&lock_);
	if (handler_.fn == handler && handler_.context == context)
	{
		handler_.fn = nullptr;
		handler_.context = nullptr;
		release = handler_release_;
		handler_acquire_ = nullptr;
		handler_release_ = nullptr;
	}
	::LeaveCriticalSection(&lock_);
	if (release)
		release(context);
}

void cancellation_signal::assign(cancellation_handler handler, void *context)
{
	this->assign(handler, context, nullptr, nullptr);
}

void cancellation_signal::assign(cancellation_handler handler, void *context,
								 cancellation_handler_ref acquire,
								 cancellation_handler_ref release)
{
	cancellation_handler_ref old_release;
	void *old_context;
	::EnterCriticalSection(&lock_);
	handler_.fn = nullptr;
	old_context = handler_.context;
	handler_.context = nullptr;
	old_release = handler_release_;
	if (acquire)
		acquire(context);
	handler_.fn = handler;
	handler_.context = context;
	handler_acquire_ = acquire;
	handler_release_ = release;
	::LeaveCriticalSection(&lock_);
	if (old_release)
		old_release(old_context);
}

void cancellation_signal::set_owner(void *owner, cancellation_slot_ref acquire,
								 cancellation_slot_ref release)
{
	owner_ = owner;
	owner_acquire_ = acquire;
	owner_release_ = release;
}

cancellation_state::cancellation_state()
	: slot_(), signal_(), notify_(nullptr), notify_context_(nullptr),
	  state_(static_cast<LONG>(cancellation_type::none))
{
}

cancellation_state::cancellation_state(const cancellation_slot &slot)
	: slot_(), signal_(), notify_(nullptr), notify_context_(nullptr),
	  state_(static_cast<LONG>(cancellation_type::none))
{
	this->connect(slot);
}

cancellation_state::~cancellation_state()
{
	if (slot_.is_connected())
		slot_.clear(&cancellation_state::on_cancel, this);
}

cancellation_slot cancellation_state::slot() const
{
	return signal_.slot();
}

cancellation_type cancellation_state::cancelled() const
{
	return static_cast<cancellation_type>(
		::InterlockedCompareExchange(const_cast<volatile LONG *>(&state_),
			0, 0));
}

void cancellation_state::clear()
{
	::InterlockedExchange(&state_,
		static_cast<LONG>(cancellation_type::none));
}

void cancellation_state::connect(const cancellation_slot &slot)
{
	if (slot_.is_connected())
		slot_.clear(&cancellation_state::on_cancel, this);
	slot_ = slot;
	if (slot_.is_connected())
		slot_.assign(&cancellation_state::on_cancel, this);
}

void cancellation_state::set_notify(cancellation_handler handler,
								 void *context)
{
	notify_ = handler;
	notify_context_ = context;
}

void cancellation_state::set_owner(void *owner, cancellation_slot_ref acquire,
								 cancellation_slot_ref release)
{
	signal_.set_owner(owner, acquire, release);
}

void cancellation_state::on_cancel(void *ctx, cancellation_type type)
{
	cancellation_state *self = static_cast<cancellation_state *>(ctx);
	::InterlockedExchange(&self->state_, static_cast<LONG>(type));
	self->signal_.emit(type);
	if (self->notify_)
		self->notify_(self->notify_context_, type);
}

