/*
  AsyncCore: minimal non-template cancellation, named after ASIO.

  cancellation_signal  -> asio::cancellation_signal
  cancellation_slot    -> asio::cancellation_slot
  cancellation_state   -> asio::cancellation_state

  The signal owns a handler object, matching ASIO's single-slot ownership
  model.  emit() calls the installed handler on every emit; state filtering
  belongs to cancellation_state.
*/

#ifndef _ASYNC_CANCELLATION_H_
#define _ASYNC_CANCELLATION_H_

#include <WinSock2.h>
#include <Windows.h>

#include <stddef.h>

enum class cancellation_type
{
	none = 0,
	terminal = 1,
	partial = 2,
	all = 3
};

typedef void (*cancellation_handler)(void *context, cancellation_type type);
typedef void (*cancellation_slot_ref)(void *context);
typedef void (*cancellation_handler_ref)(void *context);

class cancellation_entry
{
public:
	cancellation_handler fn;
	void *context;
};

class cancellation_signal;

class cancellation_slot
{
public:
	cancellation_slot();
	cancellation_slot(const cancellation_slot &other);
	cancellation_slot &operator=(const cancellation_slot &other);
	~cancellation_slot();

	bool is_connected() const;
	void clear();
	void clear(cancellation_handler handler, void *context);
	void assign(cancellation_handler handler, void *context);
	void assign(cancellation_handler handler, void *context,
				cancellation_handler_ref acquire,
				cancellation_handler_ref release);
	void emit(cancellation_type type) const;

private:
	explicit cancellation_slot(cancellation_signal *signal,
							 void *owner = nullptr,
							 cancellation_slot_ref acquire = nullptr,
							 cancellation_slot_ref release = nullptr);

	cancellation_signal *signal_;
	void *owner_;
	cancellation_slot_ref owner_acquire_;
	cancellation_slot_ref owner_release_;

	friend class cancellation_signal;
};

class cancellation_signal
{
public:
	cancellation_signal();
	~cancellation_signal();

	cancellation_signal(const cancellation_signal &) = delete;
	cancellation_signal &operator=(const cancellation_signal &) = delete;

	cancellation_slot slot() const;
	void emit(cancellation_type type);
	void set_owner(void *owner, cancellation_slot_ref acquire,
				   cancellation_slot_ref release);

private:
	friend class cancellation_slot;

	/* The lock only protects the handler registration.  emit() pins the
	 * registered context, drops the lock, and invokes the cancellation primitive
	 * outside it. */
	CRITICAL_SECTION lock_;
	cancellation_entry handler_;
	cancellation_handler_ref handler_acquire_;
	cancellation_handler_ref handler_release_;
	void *owner_;
	cancellation_slot_ref owner_acquire_;
	cancellation_slot_ref owner_release_;

	void clear();
	void clear(cancellation_handler handler, void *context);
	void assign(cancellation_handler handler, void *context);
	void assign(cancellation_handler handler, void *context,
				cancellation_handler_ref acquire,
				cancellation_handler_ref release);
};

class cancellation_state
{
public:
	cancellation_state();
	explicit cancellation_state(const cancellation_slot &slot);
	~cancellation_state();

	cancellation_state(const cancellation_state &) = delete;
	cancellation_state &operator=(const cancellation_state &) = delete;

	cancellation_slot slot() const;
	cancellation_type cancelled() const;
	void connect(const cancellation_slot &slot);
	void set_notify(cancellation_handler handler, void *context);
	void clear();
	void set_owner(void *owner, cancellation_slot_ref acquire,
				  cancellation_slot_ref release);

private:
	static void on_cancel(void *ctx, cancellation_type type);

	cancellation_slot slot_;
	cancellation_signal signal_;
	cancellation_handler notify_;
	void *notify_context_;
	volatile LONG state_;
};

#endif /* _ASYNC_CANCELLATION_H_ */

