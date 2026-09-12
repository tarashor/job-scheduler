package com.tarashor.scheduler.queue

import com.tarashor.scheduler.core.model.DeadLetterEntry
import com.tarashor.scheduler.core.model.TaskInstance
import com.tarashor.scheduler.core.model.TaskStatus
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.util.PriorityQueue
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.min
import kotlin.math.pow
import kotlin.random.Random

interface TaskQueue {
    suspend fun enqueue(task: TaskInstance)
    suspend fun poll(lookaheadMs: Long = 0L, maxWaitMs: Long = 1000L): TaskInstance?
    suspend fun requeueWithBackoff(task: TaskInstance, reason: String): TaskInstance
    suspend fun sendToDlq(task: TaskInstance, reason: String): DeadLetterEntry
    suspend fun getDlqEntries(): List<DeadLetterEntry>
    suspend fun retryDlqEntry(dlqEntryId: String): TaskInstance?
    suspend fun queueSize(): Int
    suspend fun dlqSize(): Int
}

class InMemoryTaskQueue(val numShards: Int = 16) : TaskQueue {
    private val logger = LoggerFactory.getLogger(InMemoryTaskQueue::class.java)

    private class Shard {
        val mutex = Mutex()
        val readyQueue = PriorityQueue<TaskInstance>(compareBy { it.scheduledAtEpochMs })
    }

    private val shards = Array(numShards) { Shard() }
    private val roundRobinIndex = java.util.concurrent.atomic.AtomicInteger(0)

    // Dead letter store
    private val dlq = ConcurrentHashMap<String, DeadLetterEntry>()

    private fun getShard(taskInstanceId: String): Shard {
        val shardId = (taskInstanceId.hashCode() and 0x7FFFFFFF) % numShards
        return shards[shardId]
    }

    override suspend fun enqueue(task: TaskInstance) {
        val queued = task.copy(status = TaskStatus.QUEUED)
        val shard = getShard(queued.taskInstanceId)
        shard.mutex.withLock {
            shard.readyQueue.add(queued)
            logger.info("Enqueued task instance '${task.taskInstanceId}' to shard for ${task.scheduledAtEpochMs} ms")
        }
    }

    override suspend fun poll(lookaheadMs: Long, maxWaitMs: Long): TaskInstance? {
        val deadline = System.currentTimeMillis() + maxWaitMs
        while (System.currentTimeMillis() <= deadline) {
            val now = System.currentTimeMillis()
            val threshold = now + lookaheadMs

            val startIdx = (roundRobinIndex.getAndIncrement() and 0x7FFFFFFF) % numShards
            for (i in 0 until numShards) {
                val shard = shards[(startIdx + i) % numShards]
                val taskToRun = shard.mutex.withLock {
                    val peeked = shard.readyQueue.peek()
                    if (peeked != null && peeked.scheduledAtEpochMs <= threshold) {
                        shard.readyQueue.poll()
                    } else {
                        null
                    }
                }
                if (taskToRun != null) {
                    return taskToRun
                }
            }

            kotlinx.coroutines.delay(min(50L, maxWaitMs))
        }
        return null
    }

    override suspend fun requeueWithBackoff(task: TaskInstance, reason: String): TaskInstance {
        val nextAttempt = task.attempt + 1
        if (nextAttempt > task.maxRetries) {
            logger.warn("Task '${task.taskInstanceId}' exceeded max retries ($nextAttempt > ${task.maxRetries}). Sending to DLQ.")
            sendToDlq(task, "Exceeded max retries: $reason")
            return task.copy(status = TaskStatus.DEAD_LETTER, error = reason)
        }

        // Exponential backoff with jitter
        val baseMs = 1000L
        val maxBackoffMs = 60_000L
        val exponentialDelay = (baseMs * 2.0.pow((nextAttempt - 1).toDouble())).toLong()
        val cappedDelay = min(exponentialDelay, maxBackoffMs)
        val jitter = Random.nextLong(0, min(1000L, cappedDelay / 2 + 1))
        val totalDelay = cappedDelay + jitter

        val nextScheduledAt = System.currentTimeMillis() + totalDelay
        val retryingTask = task.copy(
            attempt = nextAttempt,
            status = TaskStatus.RETRYING,
            scheduledAtEpochMs = nextScheduledAt,
            assignedWorkerId = null,
            startedAtEpochMs = null,
            error = reason
        )

        enqueue(retryingTask)
        logger.info("Requeued task '${task.taskInstanceId}' for attempt $nextAttempt in ${totalDelay}ms. Reason: $reason")
        return retryingTask
    }

    override suspend fun sendToDlq(task: TaskInstance, reason: String): DeadLetterEntry {
        val entry = DeadLetterEntry(
            id = UUID.randomUUID().toString(),
            taskInstance = task.copy(status = TaskStatus.DEAD_LETTER, error = reason),
            reason = reason,
            failedAtEpochMs = System.currentTimeMillis()
        )
        dlq[entry.id] = entry
        logger.error("Task '${task.taskInstanceId}' placed in DLQ. ID=${entry.id}, Reason: $reason")
        return entry
    }

    override suspend fun getDlqEntries(): List<DeadLetterEntry> {
        return dlq.values.sortedByDescending { it.failedAtEpochMs }
    }

    override suspend fun retryDlqEntry(dlqEntryId: String): TaskInstance? {
        val entry = dlq.remove(dlqEntryId) ?: return null
        val resetTask = entry.taskInstance.copy(
            status = TaskStatus.QUEUED,
            attempt = 1,
            error = null,
            scheduledAtEpochMs = System.currentTimeMillis()
        )
        enqueue(resetTask)
        logger.info("Re-enqueued DLQ task instance '${resetTask.taskInstanceId}' from DLQ entry '$dlqEntryId'")
        return resetTask
    }

    override suspend fun queueSize(): Int {
        var total = 0
        for (shard in shards) {
            total += shard.mutex.withLock { shard.readyQueue.size }
        }
        return total
    }
    override suspend fun dlqSize(): Int = dlq.size
}
