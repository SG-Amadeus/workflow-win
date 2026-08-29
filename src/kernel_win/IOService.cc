#include "IOService.h"
#include "Communicator.h"

#include <errno.h>

IOSession::IOSession()
	: file(INVALID_HANDLE_VALUE), buf(NULL), iov(NULL), count(0),
	  offset(0), res(0), iovcnt(0), operation(OP_NONE), service(NULL)
{
}

IOSession::~IOSession()
{
}

void IOSession::prep_pread(HANDLE hfile, void *buffer, size_t length,
						   int64_t off)
{
	this->operation = OP_PREAD;
	this->file = hfile;
	this->buf = buffer;
	this->count = length;
	this->offset = off;
	this->res = 0;
	this->iov = NULL;
	this->iovcnt = 0;
}

void IOSession::prep_pwrite(HANDLE hfile, const void *buffer, size_t length,
							int64_t off)
{
	this->operation = OP_PWRITE;
	this->file = hfile;
	this->buf = const_cast<void *>(buffer);
	this->count = length;
	this->offset = off;
	this->res = 0;
	this->iov = NULL;
	this->iovcnt = 0;
}

void IOSession::prep_preadv(HANDLE hfile, const struct iovec *vectors,
							int vcnt, int64_t off)
{
	this->operation = OP_PREADV;
	this->file = hfile;
	this->iov = vectors;
	this->iovcnt = vcnt;
	this->offset = off;
	this->res = 0;
	this->buf = NULL;
	this->count = 0;
}

void IOSession::prep_pwritev(HANDLE hfile, const struct iovec *vectors,
							 int vcnt, int64_t off)
{
	this->operation = OP_PWRITEV;
	this->file = hfile;
	this->iov = vectors;
	this->iovcnt = vcnt;
	this->offset = off;
	this->res = 0;
	this->buf = NULL;
	this->count = 0;
}

void IOSession::prep_fsync(HANDLE hfile)
{
	this->operation = OP_FSYNC;
	this->file = hfile;
	this->res = 0;
	this->buf = NULL;
	this->iov = NULL;
	this->count = 0;
	this->iovcnt = 0;
	this->offset = 0;
}

void IOSession::prep_fdatasync(HANDLE hfile)
{
	this->prep_fsync(hfile);
	this->operation = OP_FDSYNC;
}

long long IOSession::get_res() const
{
	return this->res;
}

IOService::~IOService()
{
	this->deinit();
}

int IOService::init(int maxevents)
{
	if (maxevents <= 0)
	{
		errno = EINVAL;
		return -1;
	}

	this->maxevents = maxevents;
	this->nevents = 0;
	this->unbound_called = false;
	this->owner = NULL;
	return 0;
}

void IOService::deinit()
{
	this->owner = NULL;
	this->maxevents = 0;
	this->nevents = 0;
	this->unbound_called = false;
	INIT_LIST_HEAD(&this->live_list);
}

void IOService::maybe_unbound()
{
	if (this->owner.load(std::memory_order_acquire) != NULL)
		return;

	if (this->nevents.load(std::memory_order_acquire) != 0)
		return;

	bool expected = false;
	if (this->unbound_called.compare_exchange_strong(expected, true,
												   std::memory_order_acq_rel))
		this->handle_unbound();
}

void IOService::release_session(IOSession *session)
{
	(void)session;

	int old = this->nevents.fetch_sub(1, std::memory_order_acq_rel);
	if (old <= 0)
		return;

	this->maybe_unbound();
}

int IOService::request(IOSession *session)
{
	if (!session)
	{
		errno = EINVAL;
		return -1;
	}

	if (session->prepare() < 0)
	{
		session->res = -errno;
		return -1;
	}

	AcquireSRWLockExclusive(&this->bind_lock);
	Communicator *comm = this->owner.load(std::memory_order_acquire);
	if (!comm)
	{
		ReleaseSRWLockExclusive(&this->bind_lock);
		errno = ENOTCONN;
		session->res = -errno;
		return -1;
	}

	if (this->nevents.load(std::memory_order_acquire) >= this->maxevents)
	{
		ReleaseSRWLockExclusive(&this->bind_lock);
		errno = EAGAIN;
		session->res = -errno;
		return -1;
	}

	session->service = this;
	this->nevents.fetch_add(1, std::memory_order_acq_rel);
	ReleaseSRWLockExclusive(&this->bind_lock);

	int ret = comm->io_request(session);
	if (ret < 0)
	{
		session->res = -errno;
		this->release_session(session);
		return -1;
	}

	return 0;
}

