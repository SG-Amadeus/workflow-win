/*
  Per-type operation pools owned by one Async kernel instance.

  The pool owns raw operation storage.  Its free-list link is a small node
  placed in that storage only after the concrete operation is destroyed.
  There is no allocation for a pool insertion and no operation is copied.
*/

#ifndef _ASYNC_OP_OP_POOLS_H_
#define _ASYNC_OP_OP_POOLS_H_

#include <WinSock2.h>
#include <Windows.h>
#include <stddef.h>

class connect_op;
class recv_op;
class send_op;
class recvfrom_op;
class sendto_op;
class accept_op;
class steady_timer_wait_op;
class handle_read_op;
class handle_write_op;
class handle_readv_op;
class handle_writev_op;
class executor_op;

/* Free storage is not a live operation.  It therefore cannot use the
 * operation's intrusive next_ member after the concrete destructor runs. */
struct op_pool_node
{
	op_pool_node *next;
};

class connect_op_pool
{
public:
	op_pool_node *free_list;
	CRITICAL_SECTION lock;
	size_t free_count;
	size_t max_free;
};

class recv_op_pool
{
public:
	op_pool_node *free_list;
	CRITICAL_SECTION lock;
	size_t free_count;
	size_t max_free;
};

class send_op_pool
{
public:
	op_pool_node *free_list;
	CRITICAL_SECTION lock;
	size_t free_count;
	size_t max_free;
};

class recvfrom_op_pool
{
public:
	op_pool_node *free_list;
	CRITICAL_SECTION lock;
	size_t free_count;
	size_t max_free;
};

class sendto_op_pool
{
public:
	op_pool_node *free_list;
	CRITICAL_SECTION lock;
	size_t free_count;
	size_t max_free;
};

class accept_op_pool
{
public:
	op_pool_node *free_list;
	CRITICAL_SECTION lock;
	size_t free_count;
	size_t max_free;
};

class timer_wait_op_pool
{
public:
	op_pool_node *free_list;
	CRITICAL_SECTION lock;
	size_t free_count;
	size_t max_free;
};

class handle_read_op_pool
{
public:
	op_pool_node *free_list;
	CRITICAL_SECTION lock;
	size_t free_count;
	size_t max_free;
};

class handle_write_op_pool
{
public:
	op_pool_node *free_list;
	CRITICAL_SECTION lock;
	size_t free_count;
	size_t max_free;
};

class handle_readv_op_pool
{
public:
	op_pool_node *free_list;
	CRITICAL_SECTION lock;
	size_t free_count;
	size_t max_free;
};

class handle_writev_op_pool
{
public:
	op_pool_node *free_list;
	CRITICAL_SECTION lock;
	size_t free_count;
	size_t max_free;
};

class executor_op_pool
{
public:
	op_pool_node *free_list;
	CRITICAL_SECTION lock;
	size_t free_count;
	size_t max_free;
};

class op_pools
{
public:
	connect_op_pool connect_;
	recv_op_pool recv_;
	send_op_pool send_;
	recvfrom_op_pool recvfrom_;
	sendto_op_pool sendto_;
	accept_op_pool accept_;
	timer_wait_op_pool timer_wait_;
	handle_read_op_pool handle_read_;
	handle_write_op_pool handle_write_;
	handle_readv_op_pool handle_readv_;
	handle_writev_op_pool handle_writev_;
	executor_op_pool executor_;
};

op_pools *op_pools_create();
void op_pools_destroy(op_pools *pools);

connect_op *op_pools_alloc_connect(op_pools *pools);
void op_pools_free_connect(op_pools *pools, connect_op *op);
recv_op *op_pools_alloc_recv(op_pools *pools);
void op_pools_free_recv(op_pools *pools, recv_op *op);
send_op *op_pools_alloc_send(op_pools *pools);
void op_pools_free_send(op_pools *pools, send_op *op);
recvfrom_op *op_pools_alloc_recvfrom(op_pools *pools);
void op_pools_free_recvfrom(op_pools *pools, recvfrom_op *op);
sendto_op *op_pools_alloc_sendto(op_pools *pools);
void op_pools_free_sendto(op_pools *pools, sendto_op *op);
accept_op *op_pools_alloc_accept(op_pools *pools);
void op_pools_free_accept(op_pools *pools, accept_op *op);
steady_timer_wait_op *op_pools_alloc_timer_wait(op_pools *pools);
void op_pools_free_timer_wait(op_pools *pools, steady_timer_wait_op *op);
handle_read_op *op_pools_alloc_handle_read(op_pools *pools);
void op_pools_free_handle_read(op_pools *pools, handle_read_op *op);
handle_write_op *op_pools_alloc_handle_write(op_pools *pools);
void op_pools_free_handle_write(op_pools *pools, handle_write_op *op);
handle_readv_op *op_pools_alloc_handle_readv(op_pools *pools);
void op_pools_free_handle_readv(op_pools *pools, handle_readv_op *op);
handle_writev_op *op_pools_alloc_handle_writev(op_pools *pools);
void op_pools_free_handle_writev(op_pools *pools, handle_writev_op *op);
executor_op *op_pools_alloc_executor(op_pools *pools);
void op_pools_free_executor(op_pools *pools, executor_op *op);

#endif /* _ASYNC_OP_OP_POOLS_H_ */

