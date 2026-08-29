/*
  Copyright (c) 2019 Sogou, Inc.

  Licensed under the Apache License, Version 2.0.
*/

#include <utility>
#include "WFGlobal.h"
#include "WFTaskFactory.h"

class WFFilepreadTask : public WFFileIOTask
{
public:
	WFFilepreadTask(HANDLE file, void *buf, size_t count, int64_t offset,
					IOService *service, fio_callback_t&& callback) :
		WFFileIOTask(service, std::move(callback))
	{
		this->args.file = file;
		this->args.buf = buf;
		this->args.count = count;
		this->args.offset = offset;
	}

private:
	int prepare() override
	{
		this->prep_pread(this->args.file, this->args.buf, this->args.count,
						 this->args.offset);
		return 0;
	}
};

class WFFilepwriteTask : public WFFileIOTask
{
public:
	WFFilepwriteTask(HANDLE file, const void *buf, size_t count,
					 int64_t offset, IOService *service,
					 fio_callback_t&& callback) :
		WFFileIOTask(service, std::move(callback))
	{
		this->args.file = file;
		this->args.buf = (void *)buf;
		this->args.count = count;
		this->args.offset = offset;
	}

private:
	int prepare() override
	{
		this->prep_pwrite(this->args.file, this->args.buf, this->args.count,
						  this->args.offset);
		return 0;
	}
};

class WFFilepreadvTask : public WFFileVIOTask
{
public:
	WFFilepreadvTask(HANDLE file, const struct iovec *iov, int iovcnt,
					 int64_t offset, IOService *service,
					 fvio_callback_t&& callback) :
		WFFileVIOTask(service, std::move(callback))
	{
		this->args.file = file;
		this->args.iov = iov;
		this->args.iovcnt = iovcnt;
		this->args.offset = offset;
	}

private:
	int prepare() override
	{
		this->prep_preadv(this->args.file, this->args.iov, this->args.iovcnt,
						  this->args.offset);
		return 0;
	}
};

class WFFilepwritevTask : public WFFileVIOTask
{
public:
	WFFilepwritevTask(HANDLE file, const struct iovec *iov, int iovcnt,
					  int64_t offset, IOService *service,
					  fvio_callback_t&& callback) :
		WFFileVIOTask(service, std::move(callback))
	{
		this->args.file = file;
		this->args.iov = iov;
		this->args.iovcnt = iovcnt;
		this->args.offset = offset;
	}

private:
	int prepare() override
	{
		this->prep_pwritev(this->args.file, this->args.iov,
						   this->args.iovcnt, this->args.offset);
		return 0;
	}
};

class WFFilefsyncTask : public WFFileSyncTask
{
public:
	WFFilefsyncTask(HANDLE file, IOService *service,
					fsync_callback_t&& callback) :
		WFFileSyncTask(service, std::move(callback))
	{
		this->args.file = file;
	}

private:
	int prepare() override
	{
		this->prep_fsync(this->args.file);
		return 0;
	}
};

class WFFilefdatasyncTask : public WFFileSyncTask
{
public:
	WFFilefdatasyncTask(HANDLE file, IOService *service,
						fsync_callback_t&& callback) :
		WFFileSyncTask(service, std::move(callback))
	{
		this->args.file = file;
	}

private:
	int prepare() override
	{
		this->prep_fdatasync(this->args.file);
		return 0;
	}
};

WFFileIOTask *WFTaskFactory::create_pread_task(HANDLE file, void *buf,
										   size_t count, int64_t offset,
										   fio_callback_t callback)
{
	return new WFFilepreadTask(file, buf, count, offset,
							 WFGlobal::get_io_service(), std::move(callback));
}

WFFileIOTask *WFTaskFactory::create_pwrite_task(HANDLE file, const void *buf,
											size_t count, int64_t offset,
											fio_callback_t callback)
{
	return new WFFilepwriteTask(file, buf, count, offset,
							  WFGlobal::get_io_service(), std::move(callback));
}

WFFileVIOTask *WFTaskFactory::create_preadv_task(HANDLE file,
											 const struct iovec *iov,
											 int iovcnt, int64_t offset,
											 fvio_callback_t callback)
{
	return new WFFilepreadvTask(file, iov, iovcnt, offset,
							  WFGlobal::get_io_service(), std::move(callback));
}

WFFileVIOTask *WFTaskFactory::create_pwritev_task(HANDLE file,
											  const struct iovec *iov,
											  int iovcnt, int64_t offset,
											  fvio_callback_t callback)
{
	return new WFFilepwritevTask(file, iov, iovcnt, offset,
							   WFGlobal::get_io_service(), std::move(callback));
}

WFFileSyncTask *WFTaskFactory::create_fsync_task(HANDLE file,
											 fsync_callback_t callback)
{
	return new WFFilefsyncTask(file, WFGlobal::get_io_service(),
							 std::move(callback));
}

WFFileSyncTask *WFTaskFactory::create_fdsync_task(HANDLE file,
											  fsync_callback_t callback)
{
	return new WFFilefdatasyncTask(file, WFGlobal::get_io_service(),
								 std::move(callback));
}
