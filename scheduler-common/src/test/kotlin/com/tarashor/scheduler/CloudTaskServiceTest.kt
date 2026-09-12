package com.tarashor.scheduler

import com.tarashor.scheduler.core.model.*
import com.tarashor.scheduler.queue.InMemoryTaskQueue
import com.tarashor.scheduler.service.DefaultCloudTaskService
import com.tarashor.scheduler.storage.InMemorySchedulerStorage
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import kotlin.test.*

class CloudTaskServiceTest {

    @Test
    fun `test queue lifecycle - create, pause, resume, purge, stats`() = runBlocking {
        val storage = InMemorySchedulerStorage()
        val queue = InMemoryTaskQueue()
        val service = DefaultCloudTaskService(storage, storage, storage, storage, queue)

        // 1. Create custom queue
        val queueSpec = QueueSpec(
            queueId = "billing-queue",
            rateLimits = RateLimits(maxDispatchesPerSecond = 20.0, maxConcurrentDispatches = 5),
            retryConfig = RetryConfig(maxAttempts = 3, minBackoffMs = 500)
        )
        service.createQueue(queueSpec)

        val retrieved = service.getQueue("billing-queue")
        assertNotNull(retrieved)
        assertEquals(20.0, retrieved.rateLimits.maxDispatchesPerSecond)
        assertEquals(QueueState.RUNNING, retrieved.state)

        // 2. Add tasks to queue
        service.createTask("billing-queue", TaskSpec(
            taskId = "task-1",
            target = TaskTarget.HttpRequest(url = "http://localhost:8080/api/mock/target")
        ))
        service.createTask("billing-queue", TaskSpec(
            taskId = "task-2",
            target = TaskTarget.HttpRequest(url = "http://localhost:8080/api/mock/target")
        ))

        var stats = service.getQueueStats("billing-queue")
        assertEquals(2, stats.pendingTaskCount)

        // 3. Pause queue
        service.pauseQueue("billing-queue")
        val paused = service.getQueue("billing-queue")
        assertEquals(QueueState.PAUSED, paused?.state)

        // 4. Resume queue
        service.resumeQueue("billing-queue")
        val resumed = service.getQueue("billing-queue")
        assertEquals(QueueState.RUNNING, resumed?.state)

        // 5. Purge queue
        val purgedCount = service.purgeQueue("billing-queue")
        assertEquals(2, purgedCount)

        stats = service.getQueueStats("billing-queue")
        assertEquals(0, stats.pendingTaskCount)
    }

    @Test
    fun `test task creation, deduplication, force-run, and cancellation`() = runBlocking {
        val storage = InMemorySchedulerStorage()
        val queue = InMemoryTaskQueue()
        val service = DefaultCloudTaskService(storage, storage, storage, storage, queue)

        val futureTime = System.currentTimeMillis() + 600_000L // 10 minutes in future

        // 1. Create delayed task
        val task = service.createTask("default", TaskSpec(
            taskId = "delayed-task-1",
            scheduleTimeEpochMs = futureTime,
            target = TaskTarget.HttpRequest(
                url = "https://internal.billing/v1/invoices",
                httpMethod = "POST",
                body = "{\"invoiceId\": \"INV-999\"}"
            )
        ))

        assertEquals("delayed-task-1", task.taskId)
        assertEquals(futureTime, task.scheduleTimeEpochMs)
        assertEquals(TaskStatus.QUEUED, task.status)

        // 2. Deduplication check: submitting same taskId returns existing task
        val duplicate = service.createTask("default", TaskSpec(
            taskId = "delayed-task-1",
            scheduleTimeEpochMs = futureTime + 10000,
            target = TaskTarget.HttpRequest(url = "https://different.url")
        ))
        assertEquals(task.scheduleTimeEpochMs, duplicate.scheduleTimeEpochMs)

        // 3. Force-run task now (overrides schedule delay)
        val forced = service.runTask("default", "delayed-task-1")
        assertTrue(forced.scheduleTimeEpochMs <= System.currentTimeMillis())

        // 4. Delete task
        val secondTask = service.createTask("default", TaskSpec(taskId = "to-delete"))
        assertTrue(service.deleteTask("default", "to-delete"))
        assertNull(service.getTask("default", "to-delete"))
    }
}
