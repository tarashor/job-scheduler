package com.tarashor.scheduler.storage

import com.tarashor.scheduler.core.model.*
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.sql.DriverManager
import java.util.concurrent.ConcurrentHashMap

/**
 * Database-per-Microservice Domain Stores:
 * 1. JobMetadataStore: Owned by scheduler-api for permanent Job specifications.
 * 2. RunHistoryStore: Owned by history & audit service for run executions and task instances.
 * 3. WorkerRegistry: Used for worker health monitoring, heartbeats, and capacity tracking.
 */
interface JobMetadataStore {
    fun saveJob(job: JobSpec)
    fun getJob(jobId: String): JobSpec?
    fun listJobs(): List<JobSpec>
    fun deleteJob(jobId: String): Boolean
}

interface RunHistoryStore {
    fun saveRun(run: JobRun)
    fun getRun(runId: String): JobRun?
    fun listRuns(limit: Int = 100): List<JobRun>

    fun saveTaskInstance(instance: TaskInstance)
    fun getTaskInstance(taskInstanceId: String): TaskInstance?
    fun getTaskInstancesForRun(runId: String): List<TaskInstance>
    fun findActiveTaskInstances(): List<TaskInstance>
}

interface WorkerRegistry {
    fun upsertWorker(worker: WorkerInfo)
    fun getWorker(workerId: String): WorkerInfo?
    fun listWorkers(): List<WorkerInfo>
}

interface SchedulerStorage : JobMetadataStore, RunHistoryStore, WorkerRegistry

class CompositeSchedulerStorage(
    val jobMetadataStore: JobMetadataStore,
    val runHistoryStore: RunHistoryStore,
    val workerRegistry: WorkerRegistry
) : SchedulerStorage,
    JobMetadataStore by jobMetadataStore,
    RunHistoryStore by runHistoryStore,
    WorkerRegistry by workerRegistry

class InMemoryJobMetadataStore : JobMetadataStore {
    private val jobs = ConcurrentHashMap<String, JobSpec>()
    override fun saveJob(job: JobSpec) { jobs[job.jobId] = job }
    override fun getJob(jobId: String): JobSpec? = jobs[jobId]
    override fun listJobs(): List<JobSpec> = jobs.values.sortedByDescending { it.createdAtEpochMs }
    override fun deleteJob(jobId: String): Boolean = jobs.remove(jobId) != null
}

class InMemoryRunHistoryStore : RunHistoryStore {
    private val runs = ConcurrentHashMap<String, JobRun>()
    private val taskInstances = ConcurrentHashMap<String, TaskInstance>()

    override fun saveRun(run: JobRun) { runs[run.runId] = run }
    override fun getRun(runId: String): JobRun? = runs[runId]
    override fun listRuns(limit: Int): List<JobRun> = runs.values.sortedByDescending { it.triggeredAtEpochMs }.take(limit)

    override fun saveTaskInstance(instance: TaskInstance) { taskInstances[instance.taskInstanceId] = instance }
    override fun getTaskInstance(taskInstanceId: String): TaskInstance? = taskInstances[taskInstanceId]
    override fun getTaskInstancesForRun(runId: String): List<TaskInstance> =
        taskInstances.values.filter { it.runId == runId }.sortedBy { it.scheduledAtEpochMs }
    override fun findActiveTaskInstances(): List<TaskInstance> =
        taskInstances.values.filter { it.status in setOf(TaskStatus.QUEUED, TaskStatus.RUNNING, TaskStatus.RETRYING) }
}

class InMemoryWorkerRegistry : WorkerRegistry {
    private val workers = ConcurrentHashMap<String, WorkerInfo>()
    override fun upsertWorker(worker: WorkerInfo) { workers[worker.workerId] = worker }
    override fun getWorker(workerId: String): WorkerInfo? = workers[workerId]
    override fun listWorkers(): List<WorkerInfo> = workers.values.sortedBy { it.workerId }
}

class InMemorySchedulerStorage : SchedulerStorage {
    private val jobs = ConcurrentHashMap<String, JobSpec>()
    private val runs = ConcurrentHashMap<String, JobRun>()
    private val taskInstances = ConcurrentHashMap<String, TaskInstance>()
    private val workers = ConcurrentHashMap<String, WorkerInfo>()

    override fun saveJob(job: JobSpec) {
        jobs[job.jobId] = job
    }

    override fun getJob(jobId: String): JobSpec? = jobs[jobId]
    override fun listJobs(): List<JobSpec> = jobs.values.sortedByDescending { it.createdAtEpochMs }
    override fun deleteJob(jobId: String): Boolean = jobs.remove(jobId) != null

    override fun saveRun(run: JobRun) {
        runs[run.runId] = run
    }

    override fun getRun(runId: String): JobRun? = runs[runId]
    override fun listRuns(limit: Int): List<JobRun> = runs.values
        .sortedByDescending { it.triggeredAtEpochMs }
        .take(limit)

    override fun saveTaskInstance(instance: TaskInstance) {
        taskInstances[instance.taskInstanceId] = instance
    }

    override fun getTaskInstance(taskInstanceId: String): TaskInstance? = taskInstances[taskInstanceId]

    override fun getTaskInstancesForRun(runId: String): List<TaskInstance> =
        taskInstances.values.filter { it.runId == runId }.sortedBy { it.scheduledAtEpochMs }

    override fun findActiveTaskInstances(): List<TaskInstance> =
        taskInstances.values.filter { it.status in setOf(TaskStatus.QUEUED, TaskStatus.RUNNING, TaskStatus.RETRYING) }

    override fun upsertWorker(worker: WorkerInfo) {
        workers[worker.workerId] = worker
    }

    override fun getWorker(workerId: String): WorkerInfo? = workers[workerId]
    override fun listWorkers(): List<WorkerInfo> = workers.values.sortedBy { it.workerId }
}

class SqliteSchedulerStorage(private val dbPath: String = "scheduler.db") : SchedulerStorage {
    private val logger = LoggerFactory.getLogger(SqliteSchedulerStorage::class.java)
    private val json = Json { ignoreUnknownKeys = true }
    private val url = "jdbc:sqlite:$dbPath"

    init {
        initSchema()
    }

    private fun getConnection() = DriverManager.getConnection(url)

    private fun initSchema() {
        getConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("PRAGMA journal_mode=WAL;")
                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS jobs (
                        job_id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        json_data TEXT NOT NULL,
                        created_at INTEGER NOT NULL
                    );
                    """.trimIndent()
                )
                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS job_runs (
                        run_id TEXT PRIMARY KEY,
                        job_id TEXT NOT NULL,
                        status TEXT NOT NULL,
                        triggered_at INTEGER NOT NULL,
                        json_data TEXT NOT NULL
                    );
                    """.trimIndent()
                )
                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS task_instances (
                        instance_id TEXT PRIMARY KEY,
                        run_id TEXT NOT NULL,
                        job_id TEXT NOT NULL,
                        task_id TEXT NOT NULL,
                        status TEXT NOT NULL,
                        scheduled_at INTEGER NOT NULL,
                        json_data TEXT NOT NULL
                    );
                    """.trimIndent()
                )
                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS workers (
                        worker_id TEXT PRIMARY KEY,
                        status TEXT NOT NULL,
                        last_heartbeat INTEGER NOT NULL,
                        json_data TEXT NOT NULL
                    );
                    """.trimIndent()
                )
            }
        }
        logger.info("Initialized SQLite storage at $dbPath")
    }

    override fun saveJob(job: JobSpec) {
        val payload = json.encodeToString(job)
        getConnection().use { conn ->
            conn.prepareStatement(
                "INSERT INTO jobs(job_id, name, json_data, created_at) VALUES (?, ?, ?, ?) ON CONFLICT(job_id) DO UPDATE SET name=excluded.name, json_data=excluded.json_data"
            ).use { stmt ->
                stmt.setString(1, job.jobId)
                stmt.setString(2, job.name)
                stmt.setString(3, payload)
                stmt.setLong(4, job.createdAtEpochMs)
                stmt.executeUpdate()
            }
        }
    }

    override fun getJob(jobId: String): JobSpec? {
        getConnection().use { conn ->
            conn.prepareStatement("SELECT json_data FROM jobs WHERE job_id = ?").use { stmt ->
                stmt.setString(1, jobId)
                val rs = stmt.executeQuery()
                if (rs.next()) {
                    return json.decodeFromString<JobSpec>(rs.getString("json_data"))
                }
            }
        }
        return null
    }

    override fun listJobs(): List<JobSpec> {
        val list = mutableListOf<JobSpec>()
        getConnection().use { conn ->
            conn.prepareStatement("SELECT json_data FROM jobs ORDER BY created_at DESC").use { stmt ->
                val rs = stmt.executeQuery()
                while (rs.next()) {
                    list.add(json.decodeFromString<JobSpec>(rs.getString("json_data")))
                }
            }
        }
        return list
    }

    override fun deleteJob(jobId: String): Boolean {
        getConnection().use { conn ->
            conn.prepareStatement("DELETE FROM jobs WHERE job_id = ?").use { stmt ->
                stmt.setString(1, jobId)
                return stmt.executeUpdate() > 0
            }
        }
    }

    override fun saveRun(run: JobRun) {
        val payload = json.encodeToString(run)
        getConnection().use { conn ->
            conn.prepareStatement(
                "INSERT INTO job_runs(run_id, job_id, status, triggered_at, json_data) VALUES (?, ?, ?, ?, ?) ON CONFLICT(run_id) DO UPDATE SET status=excluded.status, json_data=excluded.json_data"
            ).use { stmt ->
                stmt.setString(1, run.runId)
                stmt.setString(2, run.jobId)
                stmt.setString(3, run.status.name)
                stmt.setLong(4, run.triggeredAtEpochMs)
                stmt.setString(5, payload)
                stmt.executeUpdate()
            }
        }
    }

    override fun getRun(runId: String): JobRun? {
        getConnection().use { conn ->
            conn.prepareStatement("SELECT json_data FROM job_runs WHERE run_id = ?").use { stmt ->
                stmt.setString(1, runId)
                val rs = stmt.executeQuery()
                if (rs.next()) {
                    return json.decodeFromString<JobRun>(rs.getString("json_data"))
                }
            }
        }
        return null
    }

    override fun listRuns(limit: Int): List<JobRun> {
        val list = mutableListOf<JobRun>()
        getConnection().use { conn ->
            conn.prepareStatement("SELECT json_data FROM job_runs ORDER BY triggered_at DESC LIMIT ?").use { stmt ->
                stmt.setInt(1, limit)
                val rs = stmt.executeQuery()
                while (rs.next()) {
                    list.add(json.decodeFromString<JobRun>(rs.getString("json_data")))
                }
            }
        }
        return list
    }

    override fun saveTaskInstance(instance: TaskInstance) {
        val payload = json.encodeToString(instance)
        getConnection().use { conn ->
            conn.prepareStatement(
                "INSERT INTO task_instances(instance_id, run_id, job_id, task_id, status, scheduled_at, json_data) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(instance_id) DO UPDATE SET status=excluded.status, json_data=excluded.json_data"
            ).use { stmt ->
                stmt.setString(1, instance.taskInstanceId)
                stmt.setString(2, instance.runId)
                stmt.setString(3, instance.jobId)
                stmt.setString(4, instance.taskId)
                stmt.setString(5, instance.status.name)
                stmt.setLong(6, instance.scheduledAtEpochMs)
                stmt.setString(7, payload)
                stmt.executeUpdate()
            }
        }
    }

    override fun getTaskInstance(taskInstanceId: String): TaskInstance? {
        getConnection().use { conn ->
            conn.prepareStatement("SELECT json_data FROM task_instances WHERE instance_id = ?").use { stmt ->
                stmt.setString(1, taskInstanceId)
                val rs = stmt.executeQuery()
                if (rs.next()) {
                    return json.decodeFromString<TaskInstance>(rs.getString("json_data"))
                }
            }
        }
        return null
    }

    override fun getTaskInstancesForRun(runId: String): List<TaskInstance> {
        val list = mutableListOf<TaskInstance>()
        getConnection().use { conn ->
            conn.prepareStatement("SELECT json_data FROM task_instances WHERE run_id = ? ORDER BY scheduled_at ASC").use { stmt ->
                stmt.setString(1, runId)
                val rs = stmt.executeQuery()
                while (rs.next()) {
                    list.add(json.decodeFromString<TaskInstance>(rs.getString("json_data")))
                }
            }
        }
        return list
    }

    override fun findActiveTaskInstances(): List<TaskInstance> {
        val list = mutableListOf<TaskInstance>()
        getConnection().use { conn ->
            conn.prepareStatement("SELECT json_data FROM task_instances WHERE status IN ('QUEUED', 'RUNNING', 'RETRYING')").use { stmt ->
                val rs = stmt.executeQuery()
                while (rs.next()) {
                    list.add(json.decodeFromString<TaskInstance>(rs.getString("json_data")))
                }
            }
        }
        return list
    }

    override fun upsertWorker(worker: WorkerInfo) {
        val payload = json.encodeToString(worker)
        getConnection().use { conn ->
            conn.prepareStatement(
                "INSERT INTO workers(worker_id, status, last_heartbeat, json_data) VALUES (?, ?, ?, ?) ON CONFLICT(worker_id) DO UPDATE SET status=excluded.status, last_heartbeat=excluded.last_heartbeat, json_data=excluded.json_data"
            ).use { stmt ->
                stmt.setString(1, worker.workerId)
                stmt.setString(2, worker.status.name)
                stmt.setLong(3, worker.lastHeartbeatEpochMs)
                stmt.setString(4, payload)
                stmt.executeUpdate()
            }
        }
    }

    override fun getWorker(workerId: String): WorkerInfo? {
        getConnection().use { conn ->
            conn.prepareStatement("SELECT json_data FROM workers WHERE worker_id = ?").use { stmt ->
                stmt.setString(1, workerId)
                val rs = stmt.executeQuery()
                if (rs.next()) {
                    return json.decodeFromString<WorkerInfo>(rs.getString("json_data"))
                }
            }
        }
        return null
    }

    override fun listWorkers(): List<WorkerInfo> {
        val list = mutableListOf<WorkerInfo>()
        getConnection().use { conn ->
            conn.prepareStatement("SELECT json_data FROM workers ORDER BY worker_id ASC").use { stmt ->
                val rs = stmt.executeQuery()
                while (rs.next()) {
                    list.add(json.decodeFromString<WorkerInfo>(rs.getString("json_data")))
                }
            }
        }
        return list
    }
}

class SqliteJobMetadataStore(private val dbPath: String = "scheduler-metadata.db") : JobMetadataStore {
    private val logger = LoggerFactory.getLogger(SqliteJobMetadataStore::class.java)
    private val json = Json { ignoreUnknownKeys = true }
    private val url = "jdbc:sqlite:$dbPath"

    init {
        initSchema()
    }

    private fun getConnection() = DriverManager.getConnection(url)

    private fun initSchema() {
        getConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("PRAGMA journal_mode=WAL;")
                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS jobs (
                        job_id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        json_data TEXT NOT NULL,
                        created_at INTEGER NOT NULL
                    );
                    """.trimIndent()
                )
            }
        }
        logger.info("Initialized SQLite JobMetadataStore at $dbPath")
    }

    override fun saveJob(job: JobSpec) {
        val payload = json.encodeToString(job)
        getConnection().use { conn ->
            conn.prepareStatement(
                "INSERT INTO jobs(job_id, name, json_data, created_at) VALUES (?, ?, ?, ?) ON CONFLICT(job_id) DO UPDATE SET name=excluded.name, json_data=excluded.json_data"
            ).use { stmt ->
                stmt.setString(1, job.jobId)
                stmt.setString(2, job.name)
                stmt.setString(3, payload)
                stmt.setLong(4, job.createdAtEpochMs)
                stmt.executeUpdate()
            }
        }
    }

    override fun getJob(jobId: String): JobSpec? {
        getConnection().use { conn ->
            conn.prepareStatement("SELECT json_data FROM jobs WHERE job_id = ?").use { stmt ->
                stmt.setString(1, jobId)
                val rs = stmt.executeQuery()
                if (rs.next()) {
                    return json.decodeFromString<JobSpec>(rs.getString("json_data"))
                }
            }
        }
        return null
    }

    override fun listJobs(): List<JobSpec> {
        val list = mutableListOf<JobSpec>()
        getConnection().use { conn ->
            conn.prepareStatement("SELECT json_data FROM jobs ORDER BY created_at DESC").use { stmt ->
                val rs = stmt.executeQuery()
                while (rs.next()) {
                    list.add(json.decodeFromString<JobSpec>(rs.getString("json_data")))
                }
            }
        }
        return list
    }

    override fun deleteJob(jobId: String): Boolean {
        getConnection().use { conn ->
            conn.prepareStatement("DELETE FROM jobs WHERE job_id = ?").use { stmt ->
                stmt.setString(1, jobId)
                return stmt.executeUpdate() > 0
            }
        }
    }
}

class SqliteRunHistoryStore(private val dbPath: String = "scheduler-history.db") : RunHistoryStore {
    private val logger = LoggerFactory.getLogger(SqliteRunHistoryStore::class.java)
    private val json = Json { ignoreUnknownKeys = true }
    private val url = "jdbc:sqlite:$dbPath"

    init {
        initSchema()
    }

    private fun getConnection() = DriverManager.getConnection(url)

    private fun initSchema() {
        getConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("PRAGMA journal_mode=WAL;")
                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS job_runs (
                        run_id TEXT PRIMARY KEY,
                        job_id TEXT NOT NULL,
                        status TEXT NOT NULL,
                        triggered_at INTEGER NOT NULL,
                        json_data TEXT NOT NULL
                    );
                    """.trimIndent()
                )
                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS task_instances (
                        instance_id TEXT PRIMARY KEY,
                        run_id TEXT NOT NULL,
                        job_id TEXT NOT NULL,
                        task_id TEXT NOT NULL,
                        status TEXT NOT NULL,
                        scheduled_at INTEGER NOT NULL,
                        json_data TEXT NOT NULL
                    );
                    """.trimIndent()
                )
            }
        }
        logger.info("Initialized SQLite RunHistoryStore at $dbPath")
    }

    override fun saveRun(run: JobRun) {
        val payload = json.encodeToString(run)
        getConnection().use { conn ->
            conn.prepareStatement(
                "INSERT INTO job_runs(run_id, job_id, status, triggered_at, json_data) VALUES (?, ?, ?, ?, ?) ON CONFLICT(run_id) DO UPDATE SET status=excluded.status, json_data=excluded.json_data"
            ).use { stmt ->
                stmt.setString(1, run.runId)
                stmt.setString(2, run.jobId)
                stmt.setString(3, run.status.name)
                stmt.setLong(4, run.triggeredAtEpochMs)
                stmt.setString(5, payload)
                stmt.executeUpdate()
            }
        }
    }

    override fun getRun(runId: String): JobRun? {
        getConnection().use { conn ->
            conn.prepareStatement("SELECT json_data FROM job_runs WHERE run_id = ?").use { stmt ->
                stmt.setString(1, runId)
                val rs = stmt.executeQuery()
                if (rs.next()) {
                    return json.decodeFromString<JobRun>(rs.getString("json_data"))
                }
            }
        }
        return null
    }

    override fun listRuns(limit: Int): List<JobRun> {
        val list = mutableListOf<JobRun>()
        getConnection().use { conn ->
            conn.prepareStatement("SELECT json_data FROM job_runs ORDER BY triggered_at DESC LIMIT ?").use { stmt ->
                stmt.setInt(1, limit)
                val rs = stmt.executeQuery()
                while (rs.next()) {
                    list.add(json.decodeFromString<JobRun>(rs.getString("json_data")))
                }
            }
        }
        return list
    }

    override fun saveTaskInstance(instance: TaskInstance) {
        val payload = json.encodeToString(instance)
        getConnection().use { conn ->
            conn.prepareStatement(
                "INSERT INTO task_instances(instance_id, run_id, job_id, task_id, status, scheduled_at, json_data) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(instance_id) DO UPDATE SET status=excluded.status, json_data=excluded.json_data"
            ).use { stmt ->
                stmt.setString(1, instance.taskInstanceId)
                stmt.setString(2, instance.runId)
                stmt.setString(3, instance.jobId)
                stmt.setString(4, instance.taskId)
                stmt.setString(5, instance.status.name)
                stmt.setLong(6, instance.scheduledAtEpochMs)
                stmt.setString(7, payload)
                stmt.executeUpdate()
            }
        }
    }

    override fun getTaskInstance(taskInstanceId: String): TaskInstance? {
        getConnection().use { conn ->
            conn.prepareStatement("SELECT json_data FROM task_instances WHERE instance_id = ?").use { stmt ->
                stmt.setString(1, taskInstanceId)
                val rs = stmt.executeQuery()
                if (rs.next()) {
                    return json.decodeFromString<TaskInstance>(rs.getString("json_data"))
                }
            }
        }
        return null
    }

    override fun getTaskInstancesForRun(runId: String): List<TaskInstance> {
        val list = mutableListOf<TaskInstance>()
        getConnection().use { conn ->
            conn.prepareStatement("SELECT json_data FROM task_instances WHERE run_id = ? ORDER BY scheduled_at ASC").use { stmt ->
                stmt.setString(1, runId)
                val rs = stmt.executeQuery()
                while (rs.next()) {
                    list.add(json.decodeFromString<TaskInstance>(rs.getString("json_data")))
                }
            }
        }
        return list
    }

    override fun findActiveTaskInstances(): List<TaskInstance> {
        val list = mutableListOf<TaskInstance>()
        getConnection().use { conn ->
            conn.prepareStatement("SELECT json_data FROM task_instances WHERE status IN ('QUEUED', 'RUNNING', 'RETRYING')").use { stmt ->
                val rs = stmt.executeQuery()
                while (rs.next()) {
                    list.add(json.decodeFromString<TaskInstance>(rs.getString("json_data")))
                }
            }
        }
        return list
    }
}

class SqliteWorkerRegistry(private val dbPath: String = "scheduler-workers.db") : WorkerRegistry {
    private val logger = LoggerFactory.getLogger(SqliteWorkerRegistry::class.java)
    private val json = Json { ignoreUnknownKeys = true }
    private val url = "jdbc:sqlite:$dbPath"

    init {
        initSchema()
    }

    private fun getConnection() = DriverManager.getConnection(url)

    private fun initSchema() {
        getConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("PRAGMA journal_mode=WAL;")
                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS workers (
                        worker_id TEXT PRIMARY KEY,
                        status TEXT NOT NULL,
                        last_heartbeat INTEGER NOT NULL,
                        json_data TEXT NOT NULL
                    );
                    """.trimIndent()
                )
            }
        }
        logger.info("Initialized SQLite WorkerRegistry at $dbPath")
    }

    override fun upsertWorker(worker: WorkerInfo) {
        val payload = json.encodeToString(worker)
        getConnection().use { conn ->
            conn.prepareStatement(
                "INSERT INTO workers(worker_id, status, last_heartbeat, json_data) VALUES (?, ?, ?, ?) ON CONFLICT(worker_id) DO UPDATE SET status=excluded.status, last_heartbeat=excluded.last_heartbeat, json_data=excluded.json_data"
            ).use { stmt ->
                stmt.setString(1, worker.workerId)
                stmt.setString(2, worker.status.name)
                stmt.setLong(3, worker.lastHeartbeatEpochMs)
                stmt.setString(4, payload)
                stmt.executeUpdate()
            }
        }
    }

    override fun getWorker(workerId: String): WorkerInfo? {
        getConnection().use { conn ->
            conn.prepareStatement("SELECT json_data FROM workers WHERE worker_id = ?").use { stmt ->
                stmt.setString(1, workerId)
                val rs = stmt.executeQuery()
                if (rs.next()) {
                    return json.decodeFromString<WorkerInfo>(rs.getString("json_data"))
                }
            }
        }
        return null
    }

    override fun listWorkers(): List<WorkerInfo> {
        val list = mutableListOf<WorkerInfo>()
        getConnection().use { conn ->
            conn.prepareStatement("SELECT json_data FROM workers ORDER BY worker_id ASC").use { stmt ->
                val rs = stmt.executeQuery()
                while (rs.next()) {
                    list.add(json.decodeFromString<WorkerInfo>(rs.getString("json_data")))
                }
            }
        }
        return list
    }
}
