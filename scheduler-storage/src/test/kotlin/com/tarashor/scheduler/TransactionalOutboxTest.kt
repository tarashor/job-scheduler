package com.tarashor.scheduler

import com.tarashor.scheduler.core.model.OutboxEvent
import com.tarashor.scheduler.core.model.OutboxStatus
import com.tarashor.scheduler.core.model.TaskAction
import com.tarashor.scheduler.core.model.TaskInstance
import com.tarashor.scheduler.outbox.TransactionalOutboxDispatcher
import com.tarashor.scheduler.queue.InMemoryTaskQueue
import com.tarashor.scheduler.storage.InMemoryOutboxStore
import com.tarashor.scheduler.storage.SqliteSchedulerStorage
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.io.File
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class TransactionalOutboxTest {

    @Test
    fun `test in-memory outbox dispatching drains pending events`() = runBlocking {
        val outboxStore = InMemoryOutboxStore()
        val queue = InMemoryTaskQueue()
        val dispatcher = TransactionalOutboxDispatcher(outboxStore, queue)

        val task1 = TaskInstance(
            taskInstanceId = "outbox-task-1",
            runId = "run-1",
            jobId = "job-1",
            taskId = "t1",
            action = TaskAction.Shell("echo outbox 1"),
            scheduledAtEpochMs = System.currentTimeMillis()
        )
        val event1 = OutboxEvent(
            eventId = UUID.randomUUID().toString(),
            aggregateId = task1.taskInstanceId,
            taskInstance = task1
        )
        outboxStore.saveOutboxEvent(event1)

        val task2 = TaskInstance(
            taskInstanceId = "outbox-task-2",
            runId = "run-1",
            jobId = "job-1",
            taskId = "t2",
            action = TaskAction.Shell("echo outbox 2"),
            scheduledAtEpochMs = System.currentTimeMillis()
        )
        val event2 = OutboxEvent(
            eventId = UUID.randomUUID().toString(),
            aggregateId = task2.taskInstanceId,
            taskInstance = task2
        )
        outboxStore.saveOutboxEvent(event2)

        assertEquals(2, outboxStore.fetchPendingOutboxEvents().size)
        assertEquals(0, queue.queueSize())

        // Run outbox dispatcher batch
        val dispatched = dispatcher.dispatchPendingBatch()
        assertEquals(2, dispatched)

        // All pending events drained and present in task queue
        assertEquals(0, outboxStore.fetchPendingOutboxEvents().size)
        assertEquals(2, queue.queueSize())

        val polled = queue.poll(maxWaitMs = 100)
        assertNotNull(polled)
    }

    @Test
    fun `test sqlite durable outbox store persistence and recovery`() = runBlocking {
        val dbFile = File.createTempFile("test-outbox", ".db")
        dbFile.deleteOnExit()

        val sqliteStorage = SqliteSchedulerStorage(dbFile.absolutePath)
        val queue = InMemoryTaskQueue()
        val dispatcher = TransactionalOutboxDispatcher(sqliteStorage, queue)

        val task = TaskInstance(
            taskInstanceId = "durable-task-100",
            runId = "run-100",
            jobId = "job-100",
            taskId = "t100",
            action = TaskAction.Shell("echo sqlite outbox"),
            scheduledAtEpochMs = System.currentTimeMillis()
        )
        val event = OutboxEvent(
            eventId = "evt-100",
            aggregateId = task.taskInstanceId,
            taskInstance = task
        )
        sqliteStorage.saveOutboxEvent(event)

        val pendingBefore = sqliteStorage.fetchPendingOutboxEvents()
        assertEquals(1, pendingBefore.size)
        assertEquals(OutboxStatus.PENDING, pendingBefore[0].status)

        // Dispatch
        dispatcher.dispatchPendingBatch()

        val pendingAfter = sqliteStorage.fetchPendingOutboxEvents()
        assertEquals(0, pendingAfter.size, "Pending events should be 0 after successful dispatch")

        assertEquals(1, queue.queueSize())
    }
}
