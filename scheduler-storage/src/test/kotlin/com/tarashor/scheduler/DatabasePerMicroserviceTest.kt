package com.tarashor.scheduler

import com.tarashor.scheduler.core.model.*
import com.tarashor.scheduler.storage.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.sql.DriverManager
import kotlin.test.*

class DatabasePerMicroserviceTest {

    @Test
    fun `test isolated SqliteJobMetadataStore only manages jobs table`(@TempDir tempDir: File) {
        val dbFile = File(tempDir, "api_metadata.db")
        val jobStore = SqliteJobMetadataStore(dbFile.absolutePath)

        val job = JobSpec(
            jobId = "job-invoice-generation",
            name = "Invoice Generation DAG",
            schedule = ScheduleSpec.Cron("0 0 * * *"),
            tasks = listOf(
                TaskSpec("t1", "Fetch Orders", dependencies = emptySet(), action = TaskAction.Shell("echo fetch"))
            )
        )

        // 1. Save and retrieve
        jobStore.saveJob(job)
        val loaded = jobStore.getJob("job-invoice-generation")
        assertNotNull(loaded)
        assertEquals("Invoice Generation DAG", loaded.name)
        assertEquals(1, loaded.tasks.size)

        // 2. List jobs
        val list = jobStore.listJobs()
        assertEquals(1, list.size)
        assertEquals("job-invoice-generation", list[0].jobId)

        // 3. Verify that the DB schema ONLY contains 'jobs', proving Database-per-Microservice isolation
        val tables = getTableNames(dbFile.absolutePath)
        assertTrue("jobs" in tables, "Table 'jobs' must exist in API metadata DB")
        assertFalse("job_runs" in tables, "Table 'job_runs' must NOT exist in API metadata DB")
        assertFalse("task_instances" in tables, "Table 'task_instances' must NOT exist in API metadata DB")
        assertFalse("workers" in tables, "Table 'workers' must NOT exist in API metadata DB")

        // 4. Delete job
        val deleted = jobStore.deleteJob("job-invoice-generation")
        assertTrue(deleted)
        assertNull(jobStore.getJob("job-invoice-generation"))
    }

    @Test
    fun `test isolated SqliteRunHistoryStore only manages runs and task instances`(@TempDir tempDir: File) {
        val dbFile = File(tempDir, "runs_history.db")
        val historyStore = SqliteRunHistoryStore(dbFile.absolutePath)

        val run = JobRun(
            runId = "run-001",
            jobId = "job-100",
            status = JobStatus.RUNNING,
            triggeredAtEpochMs = 1700000000000L
        )
        historyStore.saveRun(run)

        val taskInstance = TaskInstance(
            taskInstanceId = "run-001-t1-1",
            runId = "run-001",
            jobId = "job-100",
            taskId = "t1",
            status = TaskStatus.QUEUED,
            scheduledAtEpochMs = 1700000000000L,
            action = TaskAction.Shell("echo run")
        )
        historyStore.saveTaskInstance(taskInstance)

        // Verify retrieval
        val loadedRun = historyStore.getRun("run-001")
        assertNotNull(loadedRun)
        assertEquals(JobStatus.RUNNING, loadedRun.status)

        val loadedTasks = historyStore.getTaskInstancesForRun("run-001")
        assertEquals(1, loadedTasks.size)
        assertEquals("run-001-t1-1", loadedTasks[0].taskInstanceId)

        // Verify DB schema isolation: only job_runs and task_instances exist
        val tables = getTableNames(dbFile.absolutePath)
        assertTrue("job_runs" in tables)
        assertTrue("task_instances" in tables)
        assertFalse("jobs" in tables, "Table 'jobs' must NOT exist in History DB")
        assertFalse("workers" in tables, "Table 'workers' must NOT exist in History DB")
    }

    @Test
    fun `test isolated SqliteWorkerRegistry only manages workers`(@TempDir tempDir: File) {
        val dbFile = File(tempDir, "workers.db")
        val workerRegistry = SqliteWorkerRegistry(dbFile.absolutePath)

        val worker = WorkerInfo(
            workerId = "worker-node-1",
            capacity = 8,
            currentLoad = 2,
            lastHeartbeatEpochMs = System.currentTimeMillis(),
            status = WorkerStatus.HEALTHY,
            activeTaskIds = setOf("task-1", "task-2")
        )
        workerRegistry.upsertWorker(worker)

        val loaded = workerRegistry.getWorker("worker-node-1")
        assertNotNull(loaded)
        assertEquals(8, loaded.capacity)
        assertEquals(2, loaded.currentLoad)
        assertEquals(WorkerStatus.HEALTHY, loaded.status)

        // Verify DB schema isolation
        val tables = getTableNames(dbFile.absolutePath)
        assertTrue("workers" in tables)
        assertFalse("jobs" in tables)
        assertFalse("job_runs" in tables)
    }

    @Test
    fun `test CompositeSchedulerStorage unifies isolated domain stores`(@TempDir tempDir: File) {
        val jobDb = File(tempDir, "isolated_jobs.db")
        val historyDb = File(tempDir, "isolated_history.db")
        val workerDb = File(tempDir, "isolated_workers.db")

        val jobStore = SqliteJobMetadataStore(jobDb.absolutePath)
        val historyStore = SqliteRunHistoryStore(historyDb.absolutePath)
        val workerRegistry = SqliteWorkerRegistry(workerDb.absolutePath)

        val composite = CompositeSchedulerStorage(jobStore, historyStore, workerRegistry)

        // 1. Save job through composite -> saved in jobDb
        val job = JobSpec(
            jobId = "composite-job",
            name = "Composite Test",
            schedule = ScheduleSpec.Immediate,
            tasks = listOf(TaskSpec("t1", "Step 1", dependencies = emptySet(), action = TaskAction.Shell("echo 1")))
        )
        composite.saveJob(job)
        assertNotNull(jobStore.getJob("composite-job"))
        assertEquals("Composite Test", composite.getJob("composite-job")?.name)

        // 2. Save run through composite -> saved in historyDb
        val run = JobRun("comp-run-1", "composite-job", JobStatus.PENDING, 123456L)
        composite.saveRun(run)
        assertNotNull(historyStore.getRun("comp-run-1"))
        assertEquals(JobStatus.PENDING, composite.getRun("comp-run-1")?.status)

        // 3. Save worker through composite -> saved in workerDb
        val worker = WorkerInfo(
            workerId = "comp-worker",
            capacity = 4,
            currentLoad = 0,
            lastHeartbeatEpochMs = 123456L,
            status = WorkerStatus.HEALTHY
        )
        composite.upsertWorker(worker)
        assertNotNull(workerRegistry.getWorker("comp-worker"))
        assertEquals(4, composite.getWorker("comp-worker")?.capacity)
    }

    private fun getTableNames(sqlitePath: String): Set<String> {
        val tables = mutableSetOf<String>()
        DriverManager.getConnection("jdbc:sqlite:$sqlitePath").use { conn ->
            conn.createStatement().use { stmt ->
                val rs = stmt.executeQuery("SELECT name FROM sqlite_master WHERE type='table'")
                while (rs.next()) {
                    tables.add(rs.getString("name"))
                }
            }
        }
        return tables
    }
}
