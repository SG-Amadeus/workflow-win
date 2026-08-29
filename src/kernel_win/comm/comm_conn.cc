#include "comm_conn.h"
#include "comm_request_op.h"
#include "comm_service_op.h"
#include "comm_sleep_op.h"
#include "../async/error.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include <climits>
#include <new>

FileHandleContext::FileHandleContext(HANDLE h)
	: source(h), file(), refs(1), volume_serial(0), file_index_high(0),
	  file_index_low(0), identity_valid(false), cached(false)
{
	INIT_LIST_HEAD(&list);
}

int FileHandleContext::init(io_context *io)
{
	if (!io || !source || source == INVALID_HANDLE_VALUE)
	{
		errno = EINVAL;
		return -1;
	}
	BY_HANDLE_FILE_INFORMATION info;
	memset(&info, 0, sizeof info);
	if (::GetFileInformationByHandle(source, &info))
	{
		volume_serial = info.dwVolumeSerialNumber;
		file_index_high = info.nFileIndexHigh;
		file_index_low = info.nFileIndexLow;
		identity_valid = true;
	}

	HANDLE duplicate = INVALID_HANDLE_VALUE;
	if (!::DuplicateHandle(::GetCurrentProcess(), source,
			::GetCurrentProcess(), &duplicate, 0, FALSE,
			DUPLICATE_SAME_ACCESS))
	{
		errno = async_win_error_to_errno((int)::GetLastError());
		return -1;
	}

	if (file.init(executor(*io)) != 0 || file.assign(duplicate) != 0)
	{
		int error = errno ? errno : EIO;
		file.release();
		::CloseHandle(duplicate);
		errno = error;
		return -1;
	}
	cached = !file.is_blocking() && identity_valid;
	return 0;
}

static void unregister_live_entry(CommConnEntry *entry);

CommServiceTarget *CommServiceTarget::create(CommService *service,
											 const struct sockaddr *addr,
											 socklen_t addrlen)
{
	void *mem = malloc(sizeof(CommServiceTarget));
	if (!mem)
		return nullptr;

	CommServiceTarget *target = new (mem) CommServiceTarget(service);
	if (target->init(addr, addrlen, 0, service->response_timeout) < 0)
	{
		target->~CommServiceTarget();
		free(target);
		return nullptr;
	}

	service->incref();
	return target;
}

void CommServiceTarget::incref()
{
	::InterlockedIncrement(&ref_);
}

void CommServiceTarget::release()
{
	if (::InterlockedDecrement(&ref_) == 0)
	{
		CommService *service = service_;
		this->deinit();
		this->~CommServiceTarget();
		free(this);
		service->decref();
	}
}

CommServiceTarget::CommServiceTarget(CommService *service)
	: ref_(1), service_(service)
{
}

CommServiceTarget::~CommServiceTarget()
{
}



CommConnEntry::CommConnEntry(CommunicatorImpl *i, CommSession *s,
							 CommConnection *c, CommTarget *t,
							 CommService *svc)
	: serial(), output_timer(), idle_timer(), tcp_res(), ssl_res(),
	  udp_res(),
	  conn(c), seq(0), state(CONN_STATE_CONNECTING),
	  refs(1), owner_released(0), retired(0), request_op(nullptr),
	  idle_timer_active(0),
	  session(s), target(t), service(svc),
	impl(i), tcp(nullptr), ssl_sock(nullptr),
	  udp_sock(nullptr), udp_shared(false), udp_context(nullptr),
	  udp_fromlen(0)
{
	INIT_LIST_HEAD(&list);
	INIT_LIST_HEAD(&live_list);
}

CommConnEntry::~CommConnEntry()
{
}

CommConnEntry *CommConnEntry::create(CommunicatorImpl *i, io_context *io,
									 CommSession *s,
									 CommConnection *c, CommTarget *t,
									 CommService *svc)
{
	void *mem = malloc(sizeof(CommConnEntry));
	if (!mem)
	{
		errno = ENOMEM;
		return nullptr;
	}

	CommConnEntry *entry = new (mem) CommConnEntry(i, s, c, t, svc);
	if (entry->serial.init(io) == 0)
	{
		if (entry->output_timer.init(executor(entry->serial)) == 0 &&
			entry->idle_timer.init(executor(entry->serial)) == 0)
			return entry;
	}

	int error = errno;
	entry->~CommConnEntry();
	free(entry);
	errno = error;
	return nullptr;
}

int CommConnEntry::construct_tcp()
{
	if (tcp_res.init(executor(serial)) != 0)
		return -1;
	tcp = &tcp_res;
	return 0;
}

int CommConnEntry::construct_udp()
{
	if (udp_res.init(executor(serial)) == 0)
	{
		udp_sock = &udp_res;
		return 0;
	}
	int error = errno;
	udp_sock = nullptr;
	errno = error;
	return -1;
}

int CommConnEntry::construct_ssl(SSL_CTX *ctx, int server)
{
	if (ssl_res.init(executor(serial), ctx, server) != 0)
		return -1;
	ssl_sock = &ssl_res;
	return 0;
}

void CommConnEntry::close_transport()
{
	if (tcp)
		tcp_res.close();
	if (ssl_sock)
		ssl_res.close();
	if (udp_sock && !udp_shared)
		udp_sock->close();
}

void CommConnEntry::destroy_connection()
{
	if (conn)
		delete conn;
	conn = nullptr;
}

void CommConnEntry::release()
{
	if (::InterlockedDecrement(&refs) == 0)
	{
		unregister_live_entry(this);
		/* The owner reference is also the lifetime of the bound async
		 * resources.  close() cancels I/O; destruction is delayed until all
		 * operation references have retired. */
		destroy_transport();
		this->~CommConnEntry();
		free(this);
	}
}

UdpServiceContext::UdpServiceContext(CommService *svc)
	: service(svc), serial(), socket(), sock(&socket), refs(1), fromlen(0)
{
}

int UdpServiceContext::init(io_context *io)
{
	if (serial.init(io) != 0)
		return -1;
	return socket.init(executor(serial));
}

void UdpServiceContext::acquire()
{
	::InterlockedIncrement(&refs);
}

void UdpServiceContext::release()
{
	if (::InterlockedDecrement(&refs) == 0)
	{
		this->~UdpServiceContext();
		free(this);
	}
}

TcpServiceContext::TcpServiceContext()
	: serial(), accept_timer(), acceptor(), refs(1)
{
}

int TcpServiceContext::init(io_context *io)
{
	if (serial.init(io) != 0)
		return -1;
	if (accept_timer.init(executor(serial)) != 0)
		return -1;
	return acceptor.init(executor(serial));
}

void TcpServiceContext::acquire()
{
	::InterlockedIncrement(&refs);
}

void TcpServiceContext::release()
{
	if (::InterlockedDecrement(&refs) == 0)
	{
		this->~TcpServiceContext();
		free(this);
	}
}

void CommConnEntry::release_owner()
{
	if (::InterlockedExchange(&owner_released, 1) == 0)
		this->release();
}

FileIOContext::FileIOContext(CommunicatorImpl *i, IOSession *s,
							 FileHandleContext *h)
	: impl(i), session(s), handle(h)
{
	INIT_LIST_HEAD(&live_list);
}


void CommConnEntry::destroy_transport()
{
	if (tcp)
		tcp_res.close();
	if (ssl_sock)
		ssl_res.close();
	if (udp_sock && !udp_shared)
		udp_res.close();
	if (udp_context)
		udp_context->release();
	tcp = nullptr;
	ssl_sock = nullptr;
	udp_sock = nullptr;
	udp_shared = false;
	udp_context = nullptr;
}
bool CommunicatorImpl::register_entry(CommConnEntry *entry)
{
	bool registered = false;
	AcquireSRWLockExclusive(&lifecycle_lock);
	if (!::InterlockedCompareExchange(&shutting_down, 0, 0))
	{
		list_add_tail(&entry->live_list, &live_entries);
		registered = true;
	}
	ReleaseSRWLockExclusive(&lifecycle_lock);
	return registered;
}

void CommunicatorImpl::unregister_entry(CommConnEntry *entry)
{
	AcquireSRWLockExclusive(&lifecycle_lock);
	if (!list_empty(&entry->live_list))
	{
		list_del(&entry->live_list);
		INIT_LIST_HEAD(&entry->live_list);
	}
	ReleaseSRWLockExclusive(&lifecycle_lock);
}

static void unregister_live_entry(CommConnEntry *entry)
{
	if (entry->impl)
		entry->impl->unregister_entry(entry);
}

bool CommunicatorImpl::register_service(CommService *service)
{
	bool registered = false;
	AcquireSRWLockExclusive(&lifecycle_lock);
	if (!::InterlockedCompareExchange(&shutting_down, 0, 0))
	{
		list_add_tail(&service->live_list, &live_services);
		registered = true;
	}
	ReleaseSRWLockExclusive(&lifecycle_lock);
	return registered;
}

void CommunicatorImpl::unregister_service(CommService *service)
{
	AcquireSRWLockExclusive(&lifecycle_lock);
	if (!list_empty(&service->live_list))
	{
		list_del(&service->live_list);
		INIT_LIST_HEAD(&service->live_list);
	}
	ReleaseSRWLockExclusive(&lifecycle_lock);
}

int CommunicatorImpl::bind_io_service(Communicator *owner,
									 IOService *service)
{
	if (!owner || !service)
	{
		errno = EINVAL;
		return -1;
	}

	AcquireSRWLockExclusive(&service->bind_lock);
	if (service->maxevents <= 0)
	{
		ReleaseSRWLockExclusive(&service->bind_lock);
		errno = EINVAL;
		return -1;
	}
	if (service->owner.load(std::memory_order_acquire) != nullptr)
	{
		ReleaseSRWLockExclusive(&service->bind_lock);
		errno = EALREADY;
		return -1;
	}

	AcquireSRWLockExclusive(&lifecycle_lock);
	if (!started || ::InterlockedCompareExchange(&shutting_down, 0, 0))
	{
		ReleaseSRWLockExclusive(&lifecycle_lock);
		ReleaseSRWLockExclusive(&service->bind_lock);
		errno = ECANCELED;
		return -1;
	}
	service->owner.store(owner, std::memory_order_release);
	service->unbound_called.store(false, std::memory_order_release);
	list_add_tail(&service->live_list, &live_io_services);
	ReleaseSRWLockExclusive(&lifecycle_lock);
	ReleaseSRWLockExclusive(&service->bind_lock);
	return 0;
}

void CommunicatorImpl::unbind_io_service(IOService *service)
{
	if (!service)
		return;
	AcquireSRWLockExclusive(&lifecycle_lock);
	if (!list_empty(&service->live_list))
	{
		list_del(&service->live_list);
		INIT_LIST_HEAD(&service->live_list);
	}
	ReleaseSRWLockExclusive(&lifecycle_lock);
}

void CommunicatorImpl::unbind_io_services()
{
	for (;;)
	{
		IOService *service = nullptr;
		AcquireSRWLockExclusive(&lifecycle_lock);
		if (!list_empty(&live_io_services))
		{
			service = list_entry(live_io_services.next,
									IOService, live_list);
			list_del(&service->live_list);
			INIT_LIST_HEAD(&service->live_list);
		}
		ReleaseSRWLockExclusive(&lifecycle_lock);
		if (!service)
			break;

		AcquireSRWLockExclusive(&service->bind_lock);
		service->owner.store(nullptr, std::memory_order_release);
		bool idle = service->nevents.load(std::memory_order_acquire) == 0;
		ReleaseSRWLockExclusive(&service->bind_lock);
		if (idle)
			service->maybe_unbound();
	}
}

bool CommunicatorImpl::register_file(FileIOContext *context)
{
	bool registered = false;
	AcquireSRWLockExclusive(&lifecycle_lock);
	if (!::InterlockedCompareExchange(&shutting_down, 0, 0))
	{
		list_add_tail(&context->live_list, &live_files);
		registered = true;
	}
	ReleaseSRWLockExclusive(&lifecycle_lock);
	return registered;
}

void CommunicatorImpl::unregister_file(FileIOContext *context)
{
	AcquireSRWLockExclusive(&lifecycle_lock);
	if (!list_empty(&context->live_list))
	{
		list_del(&context->live_list);
		INIT_LIST_HEAD(&context->live_list);
	}
	ReleaseSRWLockExclusive(&lifecycle_lock);
}


CommunicatorImpl::CommunicatorImpl()
	: shutting_down(0), handler_pool(nullptr), event_handler(nullptr)
{
	INIT_LIST_HEAD(&live_entries);
	INIT_LIST_HEAD(&live_services);
	INIT_LIST_HEAD(&live_io_services);
	INIT_LIST_HEAD(&live_files);
	INIT_LIST_HEAD(&live_sleeps);
	INIT_LIST_HEAD(&file_handles);
	InitializeSRWLock(&lifecycle_lock);
	InitializeSRWLock(&sleep_lock);
}


CommunicatorImpl::~CommunicatorImpl()
{
	destroy_handler_pool();
	destroy_file_handles();
}

int CommunicatorImpl::init_handler_pool(size_t threads)
{
	if (threads == 0)
		threads = 1;
	handler_pool = thrdpool_create(threads, 0);
	if (!handler_pool)
		return -1;
	return 0;
}

void CommunicatorImpl::run_pending_handler(const struct thrdpool_task *task)
{
	if (task && task->routine)
		task->routine(task->context);
}

int CommunicatorImpl::post_handler(void (*routine)(void *), void *context)
{
	if (!routine)
	{
		errno = EINVAL;
		return -1;
	}

	CommEventHandler *handler;
	AcquireSRWLockShared(&lifecycle_lock);
	handler = event_handler;
	ReleaseSRWLockShared(&lifecycle_lock);
	if (handler)
	{
		handler->schedule(routine, context);
		return 0;
	}

	if (!handler_pool)
	{
		errno = ECANCELED;
		return -1;
	}
	struct thrdpool_task task = { routine, context };
	return thrdpool_schedule(&task, handler_pool);
}

int CommunicatorImpl::post_handler(void (*routine)(void *), void *context,
								struct thrdpool_task_entry *storage)
{
	if (!routine || !storage)
	{
		errno = EINVAL;
		return -1;
	}

	CommEventHandler *handler;
	AcquireSRWLockShared(&lifecycle_lock);
	handler = event_handler;
	ReleaseSRWLockShared(&lifecycle_lock);
	if (handler)
	{
		handler->schedule(routine, context);
		return 0;
	}

	if (!handler_pool)
	{
		errno = ECANCELED;
		return -1;
	}

	struct thrdpool_task task = { routine, context };
	return thrdpool_schedule_preallocated(storage, &task, handler_pool);
}

void CommunicatorImpl::destroy_handler_pool()
{
	if (!handler_pool)
		return;
	thrdpool_destroy(&CommunicatorImpl::run_pending_handler,
					 handler_pool);
	handler_pool = nullptr;
}

FileHandleContext *CommunicatorImpl::acquire_file_handle(HANDLE handle)
{
	if (!handle || handle == INVALID_HANDLE_VALUE)
	{
		errno = EINVAL;
		return nullptr;
	}

	BY_HANDLE_FILE_INFORMATION info;
	memset(&info, 0, sizeof info);
	bool identity_valid = ::GetFileInformationByHandle(handle, &info) != 0;

	AcquireSRWLockExclusive(&lifecycle_lock);
	if (::InterlockedCompareExchange(&shutting_down, 0, 0))
	{
		ReleaseSRWLockExclusive(&lifecycle_lock);
		errno = ECANCELED;
		return nullptr;
	}
	if (identity_valid)
	{
		struct list_head *pos;
		list_for_each(pos, &file_handles)
		{
			FileHandleContext *context =
				list_entry(pos, FileHandleContext, list);
			if (context->source == handle && context->cached &&
				context->volume_serial == info.dwVolumeSerialNumber &&
				context->file_index_high == info.nFileIndexHigh &&
				context->file_index_low == info.nFileIndexLow)
			{
				::InterlockedIncrement(&context->refs);
				ReleaseSRWLockExclusive(&lifecycle_lock);
				return context;
			}
		}
	}

	void *mem = malloc(sizeof(FileHandleContext));
	if (!mem)
	{
		ReleaseSRWLockExclusive(&lifecycle_lock);
		errno = ENOMEM;
		return nullptr;
	}

	FileHandleContext *context = new (mem) FileHandleContext(handle);
	if (context->init(&kernel.get_io_context()) != 0)
	{
		int error = errno ? errno : EIO;
		context->~FileHandleContext();
		free(context);
		ReleaseSRWLockExclusive(&lifecycle_lock);
		errno = error;
		return nullptr;
	}
	if (context->cached)
	{
		::InterlockedIncrement(&context->refs);
		list_add_tail(&context->list, &file_handles);
	}
	ReleaseSRWLockExclusive(&lifecycle_lock);
	return context;
}

void CommunicatorImpl::release_file_handle(FileHandleContext *context)
{
	if (!context)
		return;

	if (::InterlockedDecrement(&context->refs) != 0)
		return;

	AcquireSRWLockExclusive(&lifecycle_lock);
	if (!list_empty(&context->list))
	{
		list_del(&context->list);
		INIT_LIST_HEAD(&context->list);
	}
	ReleaseSRWLockExclusive(&lifecycle_lock);
	context->~FileHandleContext();
	free(context);
}

void CommunicatorImpl::destroy_file_handles()
{
	AcquireSRWLockExclusive(&lifecycle_lock);
	while (!list_empty(&file_handles))
	{
		FileHandleContext *context =
			list_entry(file_handles.next, FileHandleContext, list);
		list_del(&context->list);
		INIT_LIST_HEAD(&context->list);
		ReleaseSRWLockExclusive(&lifecycle_lock);
		release_file_handle(context);
		AcquireSRWLockExclusive(&lifecycle_lock);
	}
	ReleaseSRWLockExclusive(&lifecycle_lock);
}

void CommunicatorImpl::shutdown()
{
	if (!started)
		return;

	started = false;
	::InterlockedExchange(&shutting_down, 1);
	unbind_io_services();
	struct list_head draining_entries;
	INIT_LIST_HEAD(&draining_entries);

	for (;;)
	{
		CommService *service = nullptr;
		AcquireSRWLockExclusive(&lifecycle_lock);
		if (!list_empty(&live_services))
		{
			service = list_entry(live_services.next, CommService, live_list);
			service->incref();
		}
		ReleaseSRWLockExclusive(&lifecycle_lock);
		if (!service)
			break;
		comm_service_op::unbind(service);
		service->decref();
	}

	for (;;)
	{
		CommConnEntry *entry = nullptr;
		AcquireSRWLockExclusive(&lifecycle_lock);
		if (!list_empty(&live_entries))
		{
			entry = list_entry(live_entries.next, CommConnEntry, live_list);
			list_del(&entry->live_list);
			list_add_tail(&entry->live_list, &draining_entries);
			::InterlockedIncrement(&entry->refs);
		}
		ReleaseSRWLockExclusive(&lifecycle_lock);
		if (!entry)
			break;
		comm_request_op::destroy_request(entry);
		entry->release_owner();
	}

	AcquireSRWLockExclusive(&lifecycle_lock);
	struct list_head *pos;
	list_for_each(pos, &live_files)
	{
		FileIOContext *context = list_entry(pos, FileIOContext, live_list);
		if (context->handle)
			context->handle->file.cancel();
	}
	ReleaseSRWLockExclusive(&lifecycle_lock);
	comm_sleep_op::cancel_all(this);
	kernel.deinit();
	/* IOCP workers are stopped before final Workflow callbacks are drained:
	 * no new business work can be submitted after this point. */
	CommEventHandler *handler;
	AcquireSRWLockShared(&lifecycle_lock);
	handler = event_handler;
	ReleaseSRWLockShared(&lifecycle_lock);
	if (handler)
		handler->wait();
	destroy_handler_pool();
	destroy_file_handles();
	while (!list_empty(&draining_entries))
	{
		struct list_head *pos = draining_entries.next;
		CommConnEntry *entry = list_entry(pos, CommConnEntry, live_list);
		list_del(pos);
		INIT_LIST_HEAD(pos);
		entry->release();
	}
	::InterlockedExchange(&shutting_down, 0);
}
