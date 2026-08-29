/*
 * Windows IOService upper-business contract.
 *
 * This is the Windows file-I/O counterpart of the Communicator header.
 * It intentionally contains no ASIO/IOCP/backend type; all lower execution
 * is delegated to the Windows runtime through Communicator::io_request().
 */

#ifndef _WORKFLOW_WIN_IOSERVICE_H_
#define _WORKFLOW_WIN_IOSERVICE_H_

#include <stddef.h>
#include <stdint.h>
#include <atomic>
#include <WinSock2.h>
#include <Windows.h>
#include "PlatformSocket.h"
#include "list.h"

#define IOS_STATE_SUCCESS  0
#define IOS_STATE_ERROR    1

class Communicator;
class CommunicatorImpl;
class comm_file_io_op;
class IOService;

class IOSession
{
private:
	virtual int prepare() = 0;
	virtual void handle(int state, int error) = 0;

protected:
	void prep_pread(HANDLE file, void *buf, size_t count, int64_t offset);
	void prep_pwrite(HANDLE file, const void *buf, size_t count,
					 int64_t offset);
	void prep_preadv(HANDLE file, const struct iovec *iov, int iovcnt,
					 int64_t offset);
	void prep_pwritev(HANDLE file, const struct iovec *iov, int iovcnt,
					  int64_t offset);
	void prep_fsync(HANDLE file);
	void prep_fdatasync(HANDLE file);
	long long get_res() const;

private:
	enum Operation
	{
		OP_NONE = 0,
		OP_PREAD,
		OP_PWRITE,
		OP_PREADV,
		OP_PWRITEV,
		OP_FSYNC,
		OP_FDSYNC
	};

	HANDLE file;
	void *buf;
	const struct iovec *iov;
	size_t count;
	int64_t offset;
	long long res;
	int iovcnt;
	Operation operation;
	IOService *service;

public:
	IOSession();
	virtual ~IOSession();

	friend class Communicator;
	friend class CommunicatorImpl;
	friend class comm_file_io_op;
	friend class IOService;
};

class IOService
{
public:
	int init(int maxevents);
	void deinit();
	int request(IOSession *session);

private:
	virtual void handle_stop(int error) { (void)error; }
	virtual void handle_unbound() = 0;

private:
	std::atomic<Communicator *> owner;
	int maxevents;
	std::atomic<int> nevents;
	std::atomic<bool> unbound_called;
	SRWLOCK bind_lock;
	struct list_head live_list;

public:
	IOService() : owner(NULL), maxevents(0), nevents(0), unbound_called(false)
	{
		InitializeSRWLock(&this->bind_lock);
		INIT_LIST_HEAD(&this->live_list);
	}
	virtual ~IOService();

private:
	void release_session(IOSession *session);
	void maybe_unbound();

	friend class Communicator;
	friend class CommunicatorImpl;
	friend class comm_file_io_op;
};

#endif /* _WORKFLOW_WIN_IOSERVICE_H_ */




