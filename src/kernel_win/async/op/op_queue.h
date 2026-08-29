/*
  AsyncCore: ASIO op_queue, de-templated.

  Direct port of asio::detail::op_queue with templates removed.
  It is an intrusive single-linked FIFO using win_iocp_operation::next_.
*/

#ifndef _ASYNC_OP_QUEUE_H_
#define _ASYNC_OP_QUEUE_H_

#include "win_iocp_operation.h"

class op_queue_access
{
public:
	static win_iocp_operation *next(win_iocp_operation *op)
	{
		return op->next_;
	}

	static void next(win_iocp_operation *op, win_iocp_operation *next)
	{
		op->next_ = next;
	}
};

class op_queue
{
public:
	op_queue() : front_(nullptr), back_(nullptr) {}
	op_queue(const op_queue &) = delete;
	op_queue &operator=(const op_queue &) = delete;

	~op_queue()
	{
		while (front_)
		{
			win_iocp_operation *op = front_;
			pop();
			op->destroy();
		}
	}

	bool empty() const { return front_ == nullptr; }

	bool is_enqueued(win_iocp_operation *op) const
	{
		return op_queue_access::next(op) != nullptr || back_ == op;
	}

	win_iocp_operation *front() const { return front_; }

	void pop()
	{
		if (front_)
		{
			win_iocp_operation *op = front_;
			front_ = op_queue_access::next(op);
			if (!front_)
				back_ = nullptr;
			op_queue_access::next(op, nullptr);
		}
	}

	void push(win_iocp_operation *op)
	{
		op_queue_access::next(op, nullptr);
		if (back_)
			op_queue_access::next(back_, op);
		else
			front_ = op;
		back_ = op;
	}

	void push(op_queue &other)
	{
		if (other.empty())
			return;
		if (back_)
			op_queue_access::next(back_, other.front_);
		else
			front_ = other.front_;
		back_ = other.back_;
		other.front_ = nullptr;
		other.back_ = nullptr;
	}

private:
	friend class op_queue_access;
	win_iocp_operation *front_;
	win_iocp_operation *back_;
};

#endif /* _ASYNC_OP_QUEUE_H_ */

