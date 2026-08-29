#include "op_pools.h"

#include "accept_op.h"
#include "connect_op.h"
#include "executor_op.h"
#include "handle_read_op.h"
#include "handle_write_op.h"
#include "handle_readv_op.h"
#include "handle_writev_op.h"
#include "recv_op.h"
#include "send_op.h"
#include "timer_wait_op.h"
#include "recvfrom_op.h"
#include "sendto_op.h"

#include "win_iocp_operation.h"
#include <stdlib.h>
#include <stddef.h>
#include <new>


namespace
{

#define OP_POOL_DESTROY(name, type) \
	static void name(void *p) \
	{ \
		static_cast<type *>(p)->~type(); \
	}

OP_POOL_DESTROY(destroy_connect_op, connect_op)
OP_POOL_DESTROY(destroy_recv_op, recv_op)
OP_POOL_DESTROY(destroy_send_op, send_op)
OP_POOL_DESTROY(destroy_recvfrom_op, recvfrom_op)
OP_POOL_DESTROY(destroy_sendto_op, sendto_op)
OP_POOL_DESTROY(destroy_accept_op, accept_op)
OP_POOL_DESTROY(destroy_timer_wait_op, steady_timer_wait_op)
OP_POOL_DESTROY(destroy_handle_read_op, handle_read_op)
OP_POOL_DESTROY(destroy_handle_write_op, handle_write_op)
OP_POOL_DESTROY(destroy_handle_readv_op, handle_readv_op)
OP_POOL_DESTROY(destroy_handle_writev_op, handle_writev_op)
OP_POOL_DESTROY(destroy_executor_op, executor_op)

#undef OP_POOL_DESTROY

static win_iocp_operation *pool_take(CRITICAL_SECTION *lock, size_t *count,
						op_pool_node **head)
{
	op_pool_node *node = nullptr;
	::EnterCriticalSection(lock);
	if (*head)
	{
		node = *head;
		*head = node->next;
		--*count;
	}
	::LeaveCriticalSection(lock);
	return reinterpret_cast<win_iocp_operation *>(node);
}

static bool pool_put(CRITICAL_SECTION *lock, size_t *count, size_t max_free,
							 op_pool_node **head, win_iocp_operation *op,
							 void (*destroy)(void *))
{
	bool kept = false;
	/* The concrete operation owns handler_work and other resources.  ASIO
	 * destroys that operation before its allocator storage is returned.  Do
	 * this outside the pool lock: a destructor may release tracked work and
	 * wake the io_context, and must never run while the pool is locked. */
	if (destroy)
		destroy(op);

	::EnterCriticalSection(lock);
	if (*count < max_free)
	{
		op_pool_node *node = new (op) op_pool_node;
		node->next = *head;
		*head = node;
		++*count;
		kept = true;
	}
	::LeaveCriticalSection(lock);
	return kept;
}

static void pool_init(CRITICAL_SECTION *lock, size_t *count)
{
	::InitializeCriticalSection(lock);
	*count = 0;
}

static void pool_delete(CRITICAL_SECTION *lock, size_t *count,
					 op_pool_node **head)
{
	while (*head)
	{
		op_pool_node *node = *head;
		*head = node->next;
		free(node);
	}
	*count = 0;
	::DeleteCriticalSection(lock);
}


} /* namespace */

op_pools *op_pools_create()
{
	op_pools *pools = (op_pools *)malloc(sizeof *pools);
	if (!pools)
		return nullptr;

	pools->connect_.free_list = nullptr;
	pool_init(&pools->connect_.lock, &pools->connect_.free_count);
	pools->connect_.max_free = 64;
	pools->recv_.free_list = nullptr;
	pool_init(&pools->recv_.lock, &pools->recv_.free_count);
	pools->recv_.max_free = 256;
	pools->send_.free_list = nullptr;
	pool_init(&pools->send_.lock, &pools->send_.free_count);
	pools->send_.max_free = 256;
	pools->recvfrom_.free_list = nullptr;
	pool_init(&pools->recvfrom_.lock, &pools->recvfrom_.free_count);
	pools->recvfrom_.max_free = 256;
	pools->sendto_.free_list = nullptr;
	pool_init(&pools->sendto_.lock, &pools->sendto_.free_count);
	pools->sendto_.max_free = 256;
	pools->accept_.free_list = nullptr;
	pool_init(&pools->accept_.lock, &pools->accept_.free_count);
	pools->accept_.max_free = 64;
	pools->timer_wait_.free_list = nullptr;
	pool_init(&pools->timer_wait_.lock, &pools->timer_wait_.free_count);
	pools->timer_wait_.max_free = 128;
	pools->handle_read_.free_list = nullptr;
	pool_init(&pools->handle_read_.lock, &pools->handle_read_.free_count);
	pools->handle_read_.max_free = 64;
	pools->handle_write_.free_list = nullptr;
	pool_init(&pools->handle_write_.lock, &pools->handle_write_.free_count);
	pools->handle_write_.max_free = 64;
	pools->handle_readv_.free_list = nullptr;
	pool_init(&pools->handle_readv_.lock, &pools->handle_readv_.free_count);
	pools->handle_readv_.max_free = 64;
	pools->handle_writev_.free_list = nullptr;
	pool_init(&pools->handle_writev_.lock, &pools->handle_writev_.free_count);
	pools->handle_writev_.max_free = 64;
	pools->executor_.free_list = nullptr;
	pool_init(&pools->executor_.lock, &pools->executor_.free_count);
	pools->executor_.max_free = 256;
	return pools;
}

void op_pools_destroy(op_pools *pools)
{
	if (!pools)
		return;
	pool_delete(&pools->connect_.lock, &pools->connect_.free_count,
				&pools->connect_.free_list);
	pool_delete(&pools->recv_.lock, &pools->recv_.free_count,
				&pools->recv_.free_list);
	pool_delete(&pools->send_.lock, &pools->send_.free_count,
				&pools->send_.free_list);
	pool_delete(&pools->recvfrom_.lock, &pools->recvfrom_.free_count,
				&pools->recvfrom_.free_list);
	pool_delete(&pools->sendto_.lock, &pools->sendto_.free_count,
				&pools->sendto_.free_list);
	pool_delete(&pools->accept_.lock, &pools->accept_.free_count,
				&pools->accept_.free_list);
	pool_delete(&pools->timer_wait_.lock, &pools->timer_wait_.free_count,
				&pools->timer_wait_.free_list);
	pool_delete(&pools->handle_read_.lock, &pools->handle_read_.free_count,
				&pools->handle_read_.free_list);
	pool_delete(&pools->handle_write_.lock, &pools->handle_write_.free_count,
				&pools->handle_write_.free_list);
	pool_delete(&pools->handle_readv_.lock, &pools->handle_readv_.free_count,
				&pools->handle_readv_.free_list);
	pool_delete(&pools->handle_writev_.lock, &pools->handle_writev_.free_count,
				&pools->handle_writev_.free_list);
	pool_delete(&pools->executor_.lock, &pools->executor_.free_count,
				&pools->executor_.free_list);
	free(pools);
}

connect_op *op_pools_alloc_connect(op_pools *pools)
{
	connect_op *op;
	if (!pools)
		return (connect_op *)malloc(sizeof(connect_op));
	op = (connect_op *)pool_take(&pools->connect_.lock,
		&pools->connect_.free_count, &pools->connect_.free_list);
	return op ? op : (connect_op *)malloc(sizeof(connect_op));
}

void op_pools_free_connect(op_pools *pools, connect_op *op)
{
	if (!op)
		return;
	if (!pools)
	{
		op->~connect_op();
		free(op);
		return;
	}
	bool kept = pool_put(&pools->connect_.lock,
		&pools->connect_.free_count, pools->connect_.max_free,
		&pools->connect_.free_list, op,
		&destroy_connect_op);
	if (!kept)
		free(op);
}
recv_op *op_pools_alloc_recv(op_pools *pools)
{
	recv_op *op;
	if (!pools)
		return (recv_op *)malloc(sizeof(recv_op));
	op = (recv_op *)pool_take(&pools->recv_.lock,
		&pools->recv_.free_count, &pools->recv_.free_list);
	return op ? op : (recv_op *)malloc(sizeof(recv_op));
}

void op_pools_free_recv(op_pools *pools, recv_op *op)
{
	if (!op)
		return;
	if (!pools)
	{
		op->~recv_op();
		free(op);
		return;
	}
	bool kept = pool_put(&pools->recv_.lock,
		&pools->recv_.free_count, pools->recv_.max_free,
		&pools->recv_.free_list, op,
		&destroy_recv_op);
	if (!kept)
		free(op);
}

send_op *op_pools_alloc_send(op_pools *pools)
{
	send_op *op;
	if (!pools)
		return (send_op *)malloc(sizeof(send_op));
	op = (send_op *)pool_take(&pools->send_.lock,
		&pools->send_.free_count, &pools->send_.free_list);
	return op ? op : (send_op *)malloc(sizeof(send_op));
}

void op_pools_free_send(op_pools *pools, send_op *op)
{
	if (!op)
		return;
	if (!pools)
	{
		op->~send_op();
		free(op);
		return;
	}
	bool kept = pool_put(&pools->send_.lock,
		&pools->send_.free_count, pools->send_.max_free,
		&pools->send_.free_list, op,
		&destroy_send_op);
	if (!kept)
		free(op);
}

recvfrom_op *op_pools_alloc_recvfrom(op_pools *pools)
{
	recvfrom_op *op;
	if (!pools)
		return (recvfrom_op *)malloc(sizeof(recvfrom_op));
	op = (recvfrom_op *)pool_take(&pools->recvfrom_.lock,
		&pools->recvfrom_.free_count, &pools->recvfrom_.free_list);
	return op ? op : (recvfrom_op *)malloc(sizeof(recvfrom_op));
}

void op_pools_free_recvfrom(op_pools *pools, recvfrom_op *op)
{
	if (!op)
		return;
	if (!pools)
	{
		op->~recvfrom_op();
		free(op);
		return;
	}
	bool kept = pool_put(&pools->recvfrom_.lock,
		&pools->recvfrom_.free_count, pools->recvfrom_.max_free,
		&pools->recvfrom_.free_list, op,
		&destroy_recvfrom_op);
	if (!kept)
		free(op);
}

sendto_op *op_pools_alloc_sendto(op_pools *pools)
{
	sendto_op *op;
	if (!pools)
		return (sendto_op *)malloc(sizeof(sendto_op));
	op = (sendto_op *)pool_take(&pools->sendto_.lock,
		&pools->sendto_.free_count, &pools->sendto_.free_list);
	return op ? op : (sendto_op *)malloc(sizeof(sendto_op));
}

void op_pools_free_sendto(op_pools *pools, sendto_op *op)
{
	if (!op)
		return;
	if (!pools)
	{
		op->~sendto_op();
		free(op);
		return;
	}
	bool kept = pool_put(&pools->sendto_.lock,
		&pools->sendto_.free_count, pools->sendto_.max_free,
		&pools->sendto_.free_list, op,
		&destroy_sendto_op);
	if (!kept)
		free(op);
}

accept_op *op_pools_alloc_accept(op_pools *pools)
{
	accept_op *op;
	if (!pools)
		return (accept_op *)malloc(sizeof(accept_op));
	op = (accept_op *)pool_take(&pools->accept_.lock,
		&pools->accept_.free_count, &pools->accept_.free_list);
	return op ? op : (accept_op *)malloc(sizeof(accept_op));
}

void op_pools_free_accept(op_pools *pools, accept_op *op)
{
	if (!op)
		return;
	if (!pools)
	{
		op->~accept_op();
		free(op);
		return;
	}
	bool kept = pool_put(&pools->accept_.lock,
		&pools->accept_.free_count, pools->accept_.max_free,
		&pools->accept_.free_list, op,
		&destroy_accept_op);
	if (!kept)
		free(op);
}

steady_timer_wait_op *op_pools_alloc_timer_wait(op_pools *pools)
{
	steady_timer_wait_op *op;
	if (!pools)
		return (steady_timer_wait_op *)malloc(sizeof(steady_timer_wait_op));
	op = (steady_timer_wait_op *)pool_take(&pools->timer_wait_.lock,
		&pools->timer_wait_.free_count, &pools->timer_wait_.free_list);
	return op ? op : (steady_timer_wait_op *)malloc(sizeof(steady_timer_wait_op));
}

void op_pools_free_timer_wait(op_pools *pools, steady_timer_wait_op *op)
{
	if (!op)
		return;
	if (!pools)
	{
		op->~steady_timer_wait_op();
		free(op);
		return;
	}
	bool kept = pool_put(&pools->timer_wait_.lock,
		&pools->timer_wait_.free_count, pools->timer_wait_.max_free,
		&pools->timer_wait_.free_list, op,
		&destroy_timer_wait_op);
	if (!kept)
		free(op);
}

handle_read_op *op_pools_alloc_handle_read(op_pools *pools)
{
	handle_read_op *op;
	if (!pools)
		return (handle_read_op *)malloc(sizeof(handle_read_op));
	op = (handle_read_op *)pool_take(&pools->handle_read_.lock,
		&pools->handle_read_.free_count, &pools->handle_read_.free_list);
	return op ? op : (handle_read_op *)malloc(sizeof(handle_read_op));
}

void op_pools_free_handle_read(op_pools *pools, handle_read_op *op)
{
	if (!op)
		return;
	if (!pools)
	{
		op->~handle_read_op();
		free(op);
		return;
	}
	bool kept = pool_put(&pools->handle_read_.lock, &pools->handle_read_.free_count,
		pools->handle_read_.max_free, &pools->handle_read_.free_list, op,
		&destroy_handle_read_op);
	if (!kept)
		free(op);
}

handle_write_op *op_pools_alloc_handle_write(op_pools *pools)
{
	handle_write_op *op;
	if (!pools)
		return (handle_write_op *)malloc(sizeof(handle_write_op));
	op = (handle_write_op *)pool_take(&pools->handle_write_.lock,
		&pools->handle_write_.free_count, &pools->handle_write_.free_list);
	return op ? op : (handle_write_op *)malloc(sizeof(handle_write_op));
}

void op_pools_free_handle_write(op_pools *pools, handle_write_op *op)
{
	if (!op)
		return;
	if (!pools)
	{
		op->~handle_write_op();
		free(op);
		return;
	}
	bool kept = pool_put(&pools->handle_write_.lock, &pools->handle_write_.free_count,
		pools->handle_write_.max_free, &pools->handle_write_.free_list, op,
		&destroy_handle_write_op);
	if (!kept)
		free(op);
}

handle_readv_op *op_pools_alloc_handle_readv(op_pools *pools)
{
	handle_readv_op *op;
	if (!pools)
		return (handle_readv_op *)malloc(sizeof(handle_readv_op));
	op = (handle_readv_op *)pool_take(&pools->handle_readv_.lock,
		&pools->handle_readv_.free_count, &pools->handle_readv_.free_list);
	return op ? op : (handle_readv_op *)malloc(sizeof(handle_readv_op));
}

void op_pools_free_handle_readv(op_pools *pools, handle_readv_op *op)
{
	if (!op)
		return;
	if (!pools)
	{
		op->~handle_readv_op();
		free(op);
		return;
	}
	bool kept = pool_put(&pools->handle_readv_.lock, &pools->handle_readv_.free_count,
		pools->handle_readv_.max_free, &pools->handle_readv_.free_list, op,
		&destroy_handle_readv_op);
	if (!kept)
		free(op);
}

handle_writev_op *op_pools_alloc_handle_writev(op_pools *pools)
{
	handle_writev_op *op;
	if (!pools)
		return (handle_writev_op *)malloc(sizeof(handle_writev_op));
	op = (handle_writev_op *)pool_take(&pools->handle_writev_.lock,
		&pools->handle_writev_.free_count, &pools->handle_writev_.free_list);
	return op ? op : (handle_writev_op *)malloc(sizeof(handle_writev_op));
}

void op_pools_free_handle_writev(op_pools *pools, handle_writev_op *op)
{
	if (!op)
		return;
	if (!pools)
	{
		op->~handle_writev_op();
		free(op);
		return;
	}
	bool kept = pool_put(&pools->handle_writev_.lock, &pools->handle_writev_.free_count,
		pools->handle_writev_.max_free, &pools->handle_writev_.free_list, op,
		&destroy_handle_writev_op);
	if (!kept)
		free(op);
}

executor_op *op_pools_alloc_executor(op_pools *pools)
{
	executor_op *op;
	if (!pools)
		return (executor_op *)malloc(sizeof(executor_op));
	op = (executor_op *)pool_take(&pools->executor_.lock,
		&pools->executor_.free_count, &pools->executor_.free_list);
	return op ? op : (executor_op *)malloc(sizeof(executor_op));
}

void op_pools_free_executor(op_pools *pools, executor_op *op)
{
	if (!op)
		return;
	if (!pools)
	{
		op->~executor_op();
		free(op);
		return;
	}
	bool kept = pool_put(&pools->executor_.lock,
		&pools->executor_.free_count, pools->executor_.max_free,
		&pools->executor_.free_list, op,
		&destroy_executor_op);
	if (!kept)
		free(op);
}

