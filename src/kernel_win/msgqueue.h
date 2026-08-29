/*
  Copyright (c) 2020 Sogou, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

  Author: Xie Han (xiehan@sogou-inc.com)
*/

#ifndef _MSGQUEUE_H_
#define _MSGQUEUE_H_

#include <stddef.h>

typedef struct __msgqueue msgqueue_t;

#ifdef __cplusplus
extern "C"
{
#endif

/*
 * The Windows implementation keeps the Linux Workflow queue contract:
 * intrusive link at linkoff, two-list swap, and a nonblocking shutdown mode
 * which wakes all waiters.  A nonzero maxlen enables bounded blocking put;
 * maxlen == 0 is the Linux-compatible unbounded mode used inside thrdpool.
 * Like Linux, the actual pending message count in blocking mode may reach
 * two times 'maxlen': the producer batch is not counted while a consumer
 * batch is still being drained, so the put may block only after both lists
 * are full.  'linkoff' can be positive or negative or zero; pointer
 * arithmetic happens at put/get time, and the caller guarantees the space.
 */
msgqueue_t *msgqueue_create(size_t maxlen, int linkoff);
void *msgqueue_get(msgqueue_t *queue);
void msgqueue_put(void *msg, msgqueue_t *queue);
void msgqueue_put_head(void *msg, msgqueue_t *queue);
void msgqueue_set_nonblock(msgqueue_t *queue);
void msgqueue_set_block(msgqueue_t *queue);
void msgqueue_destroy(msgqueue_t *queue);

#ifdef __cplusplus
}
#endif

#endif

