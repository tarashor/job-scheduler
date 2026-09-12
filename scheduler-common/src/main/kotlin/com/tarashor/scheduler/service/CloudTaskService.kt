package com.tarashor.scheduler.service

import com.tarashor.scheduler.core.model.*
import com.tarashor.scheduler.core.ratelimit.QueueRateLimiterRegistry
import com.tarashor.scheduler.queue.TaskQueue
import com.tarashor.scheduler.storage.OutboxStore
import com.tarashor.scheduler.storage.QueueStore
import com.tarashor.scheduler.storage.RunHistoryStore
import com.tarashor.scheduler.storage.TaskStore
import org.slf4j.LoggerFactory
import java.util.UUID

/**
 * Clean Architecture Application Service implementing Google Cloud Tasks logic:
 * - Queue lifecycle & state (RUNNING, PAUSED, Purge)
 * - Task submission with immediate or delayed scheduleTime
 * - Task deduplication by taskId (idempotency key)
 * - Force-run (bypassing delay) & cancellation
 */
interface CloudTaskService {
    fun createQueue(queue: QueueSpec): QueueSpec
    fun getQueue(queueId: String): QueueSpec?
    fun listQueues(): List<QueueSpec>
    fun deleteQueue(queueId: String): Boolean
    fun pauseQueue(queueId: String): QueueSpec
    fun resumeQueue(queueId: String): QueueSpec
    fun purgeQueue(queueId: String): Int
    fun getQueueStats(queueId: String): QueueStats

    suspend fun createTask(queueId: String, task: TaskSpec): TaskSpec
    fun getTask(queueId: String, taskId: String): TaskSpec?
    fun listTasks(queueId: String? = null, status: TaskStatus? = null, limit: Int = 100): List<TaskSpec>
    suspend fun deleteTask(queueId: String, taskId: String): Boolean
    suspend fun runTask(queueId: String, taskId: String): TaskSpec
}

class DefaultCloudTaskService(
    private val queueStore: QueueStore,
    private val taskStore: TaskStore,
    private val runHistoryStore: RunHistoryStore,
    private val outboxStore: OutboxStore,
    private val taskQueue: TaskQueue,
    private val rateLimiterRegistry: QueueRateLimiterRegistry = QueueRateLimiterRegistry()
) : CloudTaskService {
    private val logger = LoggerFactory.getLogger(DefaultCloudTaskService::class.java)

    override fun createQueue(queue: QueueSpec): QueueSpec {
        queueStore.saveQueue(queue)
        logger.info("Created Cloud Tasks queue '${queue.queueId}' (rate=${queue.rateLimits.maxDispatchesPerSecond}/s, maxConcurrent=${queue.rateLimits.maxConcurrentDispatches})")
        return queue
    }

    override fun getQueue(queueId: String): QueueSpec? = queueStore.getQueue(queueId)

    override fun listQueues(): List<QueueSpec> {
        val queues = queueStore.listQueues()
        return if (queues.isEmpty()) {
            val defaultQueue = QueueSpec(queueId = "default")
            queueStore.saveQueue(defaultQueue)
            listOf(defaultQueue)
        } else {
            queues
        }
    }

    override fun deleteQueue(queueId: String): Boolean {
        taskStore.purgeQueue(queueId)
        return queueStore.deleteQueue(queueId)
    }

    override fun pauseQueue(queueId: String): QueueSpec {
        val queue = queueStore.getQueue(queueId) ?: throw IllegalArgumentException("Queue '$queueId' not found")
        val paused = queue.copy(state = QueueState.PAUSED)
        queueStore.saveQueue(paused)
        logger.info("Queue '$queueId' PAUSED")
        return paused
    }

    override fun resumeQueue(queueId: String): QueueSpec {
        val queue = queueStore.getQueue(queueId) ?: throw IllegalArgumentException("Queue '$queueId' not found")
        val running = queue.copy(state = QueueState.RUNNING)
        queueStore.saveQueue(running)
        logger.info("Queue '$queueId' RESUMED")
        return running
    }

    override fun purgeQueue(queueId: String): Int {
        val count = taskStore.purgeQueue(queueId)
        logger.info("Purged $count tasks from queue '$queueId'")
        return count
    }

    override fun getQueueStats(queueId: String): QueueStats {
        val queue = queueStore.getQueue(queueId) ?: QueueSpec(queueId = queueId)
        val allTasks = taskStore.listTasks(queueId = queueId, limit = 1000)
        val pending = allTasks.count { it.status in setOf(TaskStatus.QUEUED, TaskStatus.SCHEDULED) }
        val running = allTasks.count { it.status == TaskStatus.RUNNING }
        val completed = allTasks.count { it.status == TaskStatus.COMPLETED }
        val failed = allTasks.count { it.status in setOf(TaskStatus.FAILED, TaskStatus.DEAD_LETTER) }

        return QueueStats(
            queueId = queueId,
            state = queue.state,
            pendingTaskCount = pending,
            runningTaskCount = running,
            completedTaskCount = completed,
            failedTaskCount = failed,
            rateLimits = queue.rateLimits,
            retryConfig = queue.retryConfig
        )
    }

    override suspend fun createTask(queueId: String, task: TaskSpec): TaskSpec {
        // Ensure queue exists
        val queue = queueStore.getQueue(queueId) ?: run {
            val autoQueue = QueueSpec(queueId = queueId)
            queueStore.saveQueue(autoQueue)
            autoQueue
        }

        val taskId = if (task.taskId.isBlank()) UUID.randomUUID().toString() else task.taskId

        // Deduplication check: named tasks prevent duplicate execution
        val existing = taskStore.getTask(taskId)
        if (existing != null && existing.status in setOf(TaskStatus.QUEUED, TaskStatus.RUNNING, TaskStatus.SCHEDULED)) {
            logger.warn("Deduplication: Task '$taskId' already exists in queue '${existing.queueId}' with status ${existing.status}")
            return existing
        }

        val now = System.currentTimeMillis()
        val scheduleTime = if (task.scheduleTimeEpochMs <= 0) now else task.scheduleTimeEpochMs
        val configuredMaxAttempts = if (task.maxAttempts > 0) task.maxAttempts else queue.retryConfig.maxAttempts

        val normalizedTask = task.copy(
            taskId = taskId,
            queueId = queueId,
            scheduleTimeEpochMs = scheduleTime,
            scheduledAtEpochMs = scheduleTime,
            status = TaskStatus.QUEUED,
            attempt = 1,
            maxAttempts = configuredMaxAttempts,
            createdAtEpochMs = now
        )

        taskStore.saveTask(normalizedTask)

        // Convert to TaskInstance for execution
        val execution = normalizedTask.toJobExecution()
        runHistoryStore.saveTaskInstance(execution)

        val outboxEvent = OutboxEvent(
            eventId = UUID.randomUUID().toString(),
            aggregateId = taskId,
            jobExecution = execution
        )
        outboxStore.saveOutboxEvent(outboxEvent)

        taskQueue.enqueue(execution)
        outboxStore.markOutboxDispatched(outboxEvent.eventId)

        logger.info("Created Cloud Task '$taskId' in queue '$queueId' (scheduled for $scheduleTime epoch ms, target=${task.target})")
        return normalizedTask
    }

    override fun getTask(queueId: String, taskId: String): TaskSpec? {
        val task = taskStore.getTask(taskId) ?: return null
        return if (task.queueId == queueId) task else null
    }

    override fun listTasks(queueId: String?, status: TaskStatus?, limit: Int): List<TaskSpec> {
        return taskStore.listTasks(queueId = queueId, status = status, limit = limit)
    }

    override suspend fun deleteTask(queueId: String, taskId: String): Boolean {
        val task = taskStore.getTask(taskId) ?: return false
        if (task.queueId != queueId) return false
        val deleted = taskStore.deleteTask(taskId)
        if (deleted) {
            val cancelled = task.copy(status = TaskStatus.CANCELLED)
            runHistoryStore.saveTaskInstance(cancelled.toJobExecution())
            logger.info("Cancelled/Deleted Cloud Task '$taskId' from queue '$queueId'")
        }
        return deleted
    }

    override suspend fun runTask(queueId: String, taskId: String): TaskSpec {
        val task = taskStore.getTask(taskId) ?: throw IllegalArgumentException("Task '$taskId' not found in queue '$queueId'")
        val now = System.currentTimeMillis()
        val forced = task.copy(
            scheduleTimeEpochMs = now,
            scheduledAtEpochMs = now,
            status = TaskStatus.QUEUED
        )
        taskStore.saveTask(forced)
        val execution = forced.toJobExecution()
        runHistoryStore.saveTaskInstance(execution)
        taskQueue.enqueue(execution)
        logger.info("Force-run triggered for Cloud Task '$taskId' in queue '$queueId' (dispatched immediately)")
        return forced
    }
}
