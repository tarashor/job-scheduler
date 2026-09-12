package com.tarashor.scheduler.storage

import com.tarashor.scheduler.core.model.QueueSpec
import com.tarashor.scheduler.core.model.QueueState

/**
 * Domain Port for persisting and configuring Cloud Tasks Queues.
 */
interface QueueStore {
    fun saveQueue(queue: QueueSpec)
    fun getQueue(queueId: String): QueueSpec?
    fun listQueues(): List<QueueSpec>
    fun deleteQueue(queueId: String): Boolean
    fun updateQueueState(queueId: String, state: QueueState): Boolean
}
