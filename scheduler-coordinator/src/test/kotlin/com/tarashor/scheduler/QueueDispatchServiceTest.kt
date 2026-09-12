package com.tarashor.scheduler

import com.tarashor.scheduler.coordinator.service.DefaultQueueDispatchService
import com.tarashor.scheduler.core.model.*
import com.tarashor.scheduler.queue.InMemoryTaskQueue
import com.tarashor.scheduler.storage.InMemorySchedulerStorage
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class QueueDispatchServiceTest {

    @Test
    fun `test paused queue prevents task dispatching`() = runBlocking {
        val storage = InMemorySchedulerStorage()
        val queue = InMemoryTaskQueue()

        // Create paused queue
        val pausedQueue = QueueSpec(
            queueId = "frozen-queue",
            state = QueueState.PAUSED
        )
        storage.saveQueue(pausedQueue)

        val task = TaskInstance(
            taskInstanceId = "task-paused-1",
            runId = "run-paused-1",
            jobId = "frozen-queue",
            taskId = "task-paused-1",
            scheduledAtEpochMs = System.currentTimeMillis() - 1000L,
            action = JobAction.Http(url = "http://localhost:8080/api/mock/target")
        )
        queue.enqueue(task)

        val dispatchService = DefaultQueueDispatchService(
            queueStore = storage,
            taskStore = storage,
            runHistoryStore = storage,
            taskQueue = queue
        )

        // Dispatch - should skip because queue is PAUSED
        dispatchService.dispatchPushTasks()

        // Verify task was NOT dequeued
        val peek = queue.poll(lookaheadMs = 0L, maxWaitMs = 100L)
        assertEquals("task-paused-1", peek?.taskInstanceId)
    }
}
