package com.tarashor.scheduler

import com.tarashor.scheduler.core.model.TaskAction
import com.tarashor.scheduler.core.model.TaskInstance
import com.tarashor.scheduler.core.model.TaskStatus
import com.tarashor.scheduler.queue.InMemoryTaskQueue
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class TaskQueueAndDLQTest {

    @Test
    fun `test priority polling by scheduled time`() = runBlocking {
        val queue = InMemoryTaskQueue()
        val now = System.currentTimeMillis()

        val task1 = TaskInstance(
            taskInstanceId = "task-1",
            runId = "run-1",
            jobId = "job-1",
            taskId = "t1",
            action = TaskAction.Shell("echo 1"),
            scheduledAtEpochMs = now + 100
        )

        val task2 = TaskInstance(
            taskInstanceId = "task-2",
            runId = "run-1",
            jobId = "job-1",
            taskId = "t2",
            action = TaskAction.Shell("echo 2"),
            scheduledAtEpochMs = now - 50 // Ready earlier
        )

        queue.enqueue(task1)
        queue.enqueue(task2)

        val polled1 = queue.poll(maxWaitMs = 100)
        assertNotNull(polled1)
        assertEquals("task-2", polled1.taskInstanceId, "Task with earlier scheduled time should be polled first")
    }

    @Test
    fun `test retry with backoff and DLQ routing`() = runBlocking {
        val queue = InMemoryTaskQueue()
        val now = System.currentTimeMillis()

        val task = TaskInstance(
            taskInstanceId = "flaky-task-1",
            runId = "run-1",
            jobId = "job-1",
            taskId = "t-flaky",
            attempt = 1,
            maxRetries = 2,
            action = TaskAction.Shell("exit 1"),
            scheduledAtEpochMs = now
        )

        // Attempt 1 fails -> requeued for attempt 2
        val retried = queue.requeueWithBackoff(task, "Network failure")
        assertEquals(2, retried.attempt)
        assertEquals(TaskStatus.RETRYING, retried.status)
        assertEquals(0, queue.dlqSize())

        // Attempt 2 fails -> exhausted retries -> sent to DLQ
        val terminal = queue.requeueWithBackoff(retried, "Network failure again")
        assertEquals(TaskStatus.DEAD_LETTER, terminal.status)
        assertEquals(1, queue.dlqSize())

        val dlqEntries = queue.getDlqEntries()
        assertEquals(1, dlqEntries.size)
        assertEquals("flaky-task-1", dlqEntries.first().taskInstance.taskInstanceId)

        // Replay from DLQ
        val replayed = queue.retryDlqEntry(dlqEntries.first().id)
        assertNotNull(replayed)
        assertEquals(1, replayed.attempt)
        assertEquals(0, queue.dlqSize())
        assertEquals(2, queue.queueSize(), "Queue contains the previous scheduled attempt plus the replayed DLQ task")
    }
}
