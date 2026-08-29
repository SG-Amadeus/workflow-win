#include "executor_op.h"
#include "op_pools.h"

#include <stdlib.h>

void executor_op::do_complete(void *owner, win_iocp_operation *base,
							  async_error_code /*error*/, size_t /*bytes*/)
{
	executor_op *self = static_cast<executor_op *>(base);
	void (*routine)(void *) = self->routine_;
	void (*destroy)(void *) = self->destroy_;
	void *context = self->context_;
	op_pools *pools = self->pools_;

	self->routine_ = nullptr;
	self->destroy_ = nullptr;
	self->context_ = nullptr;
	self->pools_ = nullptr;
	op_pools_free_executor(pools, self);

	if (owner && routine)
		routine(context);
	else if (!owner && destroy)
		destroy(context);
}

