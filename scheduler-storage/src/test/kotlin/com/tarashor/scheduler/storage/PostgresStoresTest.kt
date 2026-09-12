package com.tarashor.scheduler.storage

import com.tarashor.scheduler.core.model.*
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class PostgresStoresTest {

    private lateinit var storage: PostgresSchedulerStorage
    private val dbName = "pgtest_${UUID.randomUUID().toString().replace("-", "")}"
    private val testJdbcUrl = "jdbc:sqlite:file:$dbName?mode=memory&cache=shared"

    @BeforeEach
    fun setup() {
        storage = PostgresSchedulerStorage(testJdbcUrl, "sa", "")
    }

    @AfterEach
    fun teardown() {
        storage.close()
    }

    @Test
    fun `test job metadata store CRUD and conflict upsert`() {
        val job = JobSpec(
            jobId = "job-pg-1",
            name = "Test Postgres Job",
            schedule = ScheduleSpec.Cron("0 0 * * *"),
            action = JobAction.Shell("echo test")
        )

        storage.saveJob(job)

        val retrieved = storage.getJob("job-pg-1")
        assertNotNull(retrieved)
        assertEquals("Test Postgres Job", retrieved.name)
        assertEquals("0 0 * * *", (retrieved.schedule as ScheduleSpec.Cron).cronExpression)

        // Upsert test
        val updated = job.copy(name = "Updated Postgres Job")
        storage.saveJob(updated)

        val retrievedUpdated = storage.getJob("job-pg-1")
        assertNotNull(retrievedUpdated)
        assertEquals("Updated Postgres Job", retrievedUpdated.name)

        // List
        val all = storage.listJobs()
        assertEquals(1, all.size)

        // Delete
        val deleted = storage.deleteJob("job-pg-1")
        assertTrue(deleted)
        assertNull(storage.getJob("job-pg-1"))
    }

    @Test
    fun `test run history store and task instances`() {
        val run = JobRun(
            runId = "run-pg-1",
            jobId = "job-pg-1",
            status = JobStatus.RUNNING,
            triggeredAtEpochMs = System.currentTimeMillis()
        )
        storage.saveRun(run)

        val fetchedRun = storage.getRun("run-pg-1")
        assertNotNull(fetchedRun)
        assertEquals(JobStatus.RUNNING, fetchedRun.status)

        // Update run status
        val completedRun = run.copy(status = JobStatus.COMPLETED)
        storage.saveRun(completedRun)
        assertEquals(JobStatus.COMPLETED, storage.getRun("run-pg-1")?.status)

        // Task instance
        val task = TaskInstance(
            taskInstanceId = "ti-pg-1",
            runId = "run-pg-1",
            jobId = "job-pg-1",
            taskId = "task-1",
            status = TaskStatus.RUNNING,
            action = TaskAction.Shell("echo task"),
            scheduledAtEpochMs = System.currentTimeMillis()
        )
        storage.saveTaskInstance(task)

        val active = storage.findActiveTaskInstances()
        assertEquals(1, active.size)
        assertEquals("ti-pg-1", active[0].taskInstanceId)

        val runTasks = storage.getTaskInstancesForRun("run-pg-1")
        assertEquals(1, runTasks.size)
    }

    @Test
    fun `test worker registry upsert and listing`() {
        val worker = WorkerInfo(
            workerId = "worker-pg-1",
            capacity = 8,
            currentLoad = 2,
            lastHeartbeatEpochMs = System.currentTimeMillis(),
            status = WorkerStatus.HEALTHY,
            activeTaskIds = setOf("task-1")
        )
        storage.upsertWorker(worker)

        val fetched = storage.getWorker("worker-pg-1")
        assertNotNull(fetched)
        assertEquals(8, fetched.capacity)
        assertEquals(2, fetched.currentLoad)

        val list = storage.listWorkers()
        assertEquals(1, list.size)
    }

    @Test
    fun `test outbox store transactional lifecycle`() {
        val task = TaskInstance(
            taskInstanceId = "ti-pg-outbox",
            runId = "run-pg-1",
            jobId = "job-pg-1",
            taskId = "task-1",
            action = TaskAction.Shell("echo task"),
            scheduledAtEpochMs = System.currentTimeMillis()
        )
        val event = OutboxEvent(
            eventId = "evt-pg-1",
            aggregateId = "run-pg-1",
            taskInstance = task
        )
        storage.saveOutboxEvent(event)

        val pending = storage.fetchPendingOutboxEvents(10)
        assertEquals(1, pending.size)
        assertEquals("evt-pg-1", pending[0].eventId)
        assertEquals(OutboxStatus.PENDING, pending[0].status)

        val now = System.currentTimeMillis()
        storage.markOutboxDispatched("evt-pg-1", now)

        val pendingAfter = storage.fetchPendingOutboxEvents(10)
        assertTrue(pendingAfter.isEmpty())
    }
}
