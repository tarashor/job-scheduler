package com.tarashor.scheduler.storage

import com.tarashor.scheduler.core.model.*
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.sql.DriverManager

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
                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS outbox_events (
                        event_id TEXT PRIMARY KEY,
                        aggregate_id TEXT NOT NULL,
                        status TEXT NOT NULL,
                        created_at INTEGER NOT NULL,
                        dispatched_at INTEGER,
                        json_data TEXT NOT NULL
                    );
                    """.trimIndent()
                )

                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS queues (
                        queue_id TEXT PRIMARY KEY,
                        state TEXT NOT NULL,
                        json_data TEXT NOT NULL,
                        created_at INTEGER NOT NULL
                    );
                    """.trimIndent()
                )

                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cloud_tasks (
                        task_id TEXT PRIMARY KEY,
                        queue_id TEXT NOT NULL,
                        status TEXT NOT NULL,
                        schedule_time INTEGER NOT NULL,
                        created_at INTEGER NOT NULL,
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

    override fun saveOutboxEvent(event: OutboxEvent) {
        val payload = json.encodeToString(event)
        getConnection().use { conn ->
            conn.prepareStatement(
                "INSERT INTO outbox_events(event_id, aggregate_id, status, created_at, dispatched_at, json_data) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(event_id) DO UPDATE SET status=excluded.status, dispatched_at=excluded.dispatched_at, json_data=excluded.json_data"
            ).use { stmt ->
                stmt.setString(1, event.eventId)
                stmt.setString(2, event.aggregateId)
                stmt.setString(3, event.status.name)
                stmt.setLong(4, event.createdAtEpochMs)
                val dispatchedAt = event.dispatchedAtEpochMs
                if (dispatchedAt != null) {
                    stmt.setLong(5, dispatchedAt)
                } else {
                    stmt.setNull(5, java.sql.Types.BIGINT)
                }
                stmt.setString(6, payload)
                stmt.executeUpdate()
            }
        }
    }

    override fun fetchPendingOutboxEvents(limit: Int): List<OutboxEvent> {
        val list = mutableListOf<OutboxEvent>()
        getConnection().use { conn ->
            conn.prepareStatement("SELECT json_data FROM outbox_events WHERE status = 'PENDING' ORDER BY created_at ASC LIMIT ?").use { stmt ->
                stmt.setInt(1, limit)
                val rs = stmt.executeQuery()
                while (rs.next()) {
                    list.add(json.decodeFromString<OutboxEvent>(rs.getString("json_data")))
                }
            }
        }
        return list
    }

    override fun markOutboxDispatched(eventId: String, dispatchedAtEpochMs: Long) {
        val current = fetchPendingOutboxEvents(1000).find { it.eventId == eventId }
        val updatedPayload = current?.copy(
            status = OutboxStatus.DISPATCHED,
            dispatchedAtEpochMs = dispatchedAtEpochMs
        )?.let { json.encodeToString(it) }

        getConnection().use { conn ->
            conn.prepareStatement(
                "UPDATE outbox_events SET status = 'DISPATCHED', dispatched_at = ?, json_data = COALESCE(?, json_data) WHERE event_id = ?"
            ).use { stmt ->
                stmt.setLong(1, dispatchedAtEpochMs)
                if (updatedPayload != null) stmt.setString(2, updatedPayload) else stmt.setNull(2, java.sql.Types.VARCHAR)
                stmt.setString(3, eventId)
                stmt.executeUpdate()
            }
        }
    }

    override fun saveQueue(queue: QueueSpec) {
        val payload = json.encodeToString(queue)
        getConnection().use { conn ->
            conn.prepareStatement(
                "INSERT INTO queues(queue_id, state, json_data, created_at) VALUES (?, ?, ?, ?) ON CONFLICT(queue_id) DO UPDATE SET state=excluded.state, json_data=excluded.json_data"
            ).use { stmt ->
                stmt.setString(1, queue.queueId)
                stmt.setString(2, queue.state.name)
                stmt.setString(3, payload)
                stmt.setLong(4, queue.createdAtEpochMs)
                stmt.executeUpdate()
            }
        }
    }

    override fun getQueue(queueId: String): QueueSpec? {
        getConnection().use { conn ->
            conn.prepareStatement("SELECT json_data FROM queues WHERE queue_id = ?").use { stmt ->
                stmt.setString(1, queueId)
                val rs = stmt.executeQuery()
                if (rs.next()) return json.decodeFromString<QueueSpec>(rs.getString("json_data"))
            }
        }
        return null
    }

    override fun listQueues(): List<QueueSpec> {
        val list = mutableListOf<QueueSpec>()
        getConnection().use { conn ->
            conn.prepareStatement("SELECT json_data FROM queues ORDER BY queue_id ASC").use { stmt ->
                val rs = stmt.executeQuery()
                while (rs.next()) {
                    list.add(json.decodeFromString<QueueSpec>(rs.getString("json_data")))
                }
            }
        }
        return list
    }

    override fun deleteQueue(queueId: String): Boolean {
        getConnection().use { conn ->
            conn.prepareStatement("DELETE FROM queues WHERE queue_id = ?").use { stmt ->
                stmt.setString(1, queueId)
                return stmt.executeUpdate() > 0
            }
        }
    }

    override fun updateQueueState(queueId: String, state: QueueState): Boolean {
        val existing = getQueue(queueId) ?: return false
        saveQueue(existing.copy(state = state))
        return true
    }

    override fun saveTask(task: TaskSpec) {
        val payload = json.encodeToString(task)
        getConnection().use { conn ->
            conn.prepareStatement(
                "INSERT INTO cloud_tasks(task_id, queue_id, status, schedule_time, created_at, json_data) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(task_id) DO UPDATE SET queue_id=excluded.queue_id, status=excluded.status, schedule_time=excluded.schedule_time, json_data=excluded.json_data"
            ).use { stmt ->
                stmt.setString(1, task.taskId)
                stmt.setString(2, task.queueId)
                stmt.setString(3, task.status.name)
                stmt.setLong(4, task.scheduledAtEpochMs)
                stmt.setLong(5, task.createdAtEpochMs)
                stmt.setString(6, payload)
                stmt.executeUpdate()
            }
        }
    }

    override fun getTask(taskId: String): TaskSpec? {
        getConnection().use { conn ->
            conn.prepareStatement("SELECT json_data FROM cloud_tasks WHERE task_id = ?").use { stmt ->
                stmt.setString(1, taskId)
                val rs = stmt.executeQuery()
                if (rs.next()) return json.decodeFromString<TaskSpec>(rs.getString("json_data"))
            }
        }
        return null
    }

    override fun listTasks(queueId: String?, status: TaskStatus?, limit: Int): List<TaskSpec> {
        val list = mutableListOf<TaskSpec>()
        val conditions = mutableListOf<String>()
        if (!queueId.isNullOrBlank()) conditions.add("queue_id = ?")
        if (status != null) conditions.add("status = ?")
        val whereClause = if (conditions.isNotEmpty()) "WHERE " + conditions.joinToString(" AND ") else ""
        val sql = "SELECT json_data FROM cloud_tasks $whereClause ORDER BY created_at DESC LIMIT ?"

        getConnection().use { conn ->
            conn.prepareStatement(sql).use { stmt ->
                var paramIdx = 1
                if (!queueId.isNullOrBlank()) stmt.setString(paramIdx++, queueId)
                if (status != null) stmt.setString(paramIdx++, status.name)
                stmt.setInt(paramIdx, limit)
                val rs = stmt.executeQuery()
                while (rs.next()) {
                    list.add(json.decodeFromString<TaskSpec>(rs.getString("json_data")))
                }
            }
        }
        return list
    }

    override fun deleteTask(taskId: String): Boolean {
        getConnection().use { conn ->
            conn.prepareStatement("DELETE FROM cloud_tasks WHERE task_id = ?").use { stmt ->
                stmt.setString(1, taskId)
                return stmt.executeUpdate() > 0
            }
        }
    }

    override fun purgeQueue(queueId: String): Int {
        getConnection().use { conn ->
            conn.prepareStatement("DELETE FROM cloud_tasks WHERE queue_id = ? AND status IN ('QUEUED', 'SCHEDULED')").use { stmt ->
                stmt.setString(1, queueId)
                return stmt.executeUpdate()
            }
        }
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
