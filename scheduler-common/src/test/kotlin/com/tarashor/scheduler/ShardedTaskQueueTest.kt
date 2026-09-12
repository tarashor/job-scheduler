package com.tarashor.scheduler

import com.tarashor.scheduler.core.model.TaskAction
import com.tarashor.scheduler.core.model.TaskInstance
import com.tarashor.scheduler.queue.InMemoryTaskQueue
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class ShardedTaskQueueTest {

    @Test
    fun `test prefetch lookahead polling window`() = runBlocking {
        val queue = InMemoryTaskQueue(numShards = 4)
        val now = System.currentTimeMillis()

        // Task 1: ready now
        val taskNow = TaskInstance(
            taskInstanceId = "task-now",
            runId = "run-1",
            jobId = "job-1",
            taskId = "t-now",
            action = TaskAction.Shell("echo now"),
            scheduledAtEpochMs = now
        )

        // Task 2: due in 3 seconds (within 5-sec lookahead)
        val taskSoon = TaskInstance(
            taskInstanceId = "task-soon",
            runId = "run-1",
            jobId = "job-1",
            taskId = "t-soon",
            action = TaskAction.Shell("echo soon"),
            scheduledAtEpochMs = now + 3000
        )

        // Task 3: due in 10 seconds (outside 5-sec lookahead)
        val taskFar = TaskInstance(
            taskInstanceId = "task-far",
            runId = "run-1",
            jobId = "job-1",
            taskId = "t-far",
            action = TaskAction.Shell("echo far"),
            scheduledAtEpochMs = now + 10_000
        )

        queue.enqueue(taskNow)
        queue.enqueue(taskSoon)
        queue.enqueue(taskFar)
        assertEquals(3, queue.queueSize())

        // Standard poll (lookahead = 0): only taskNow should be polled
        val polled1 = queue.poll(lookaheadMs = 0L, maxWaitMs = 100)
        assertNotNull(polled1)
        assertEquals("task-now", polled1.taskInstanceId)

        // Standard poll again: null because taskSoon is not ready at lookahead=0
        val polledNull = queue.poll(lookaheadMs = 0L, maxWaitMs = 100)
        assertNull(polledNull)

        // Prefetch poll with lookahead = 5000ms: taskSoon (due in 3000ms) is returned!
        val polled2 = queue.poll(lookaheadMs = 5000L, maxWaitMs = 100)
        assertNotNull(polled2)
        assertEquals("task-soon", polled2.taskInstanceId)

        // Prefetch poll with lookahead = 5000ms again: null because taskFar is at 10_000ms
        val polledStillNull = queue.poll(lookaheadMs = 5000L, maxWaitMs = 100)
        assertNull(polledStillNull)

        assertEquals(1, queue.queueSize(), "Only taskFar remains in queue")
    }
}
