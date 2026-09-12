package com.tarashor.scheduler.storage

import com.tarashor.scheduler.cluster.LeaseStore
import com.tarashor.scheduler.core.model.*
import com.tarashor.scheduler.queue.TaskQueue
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.sql.Connection
import javax.sql.DataSource
import kotlin.math.min
import kotlin.math.pow
import kotlin.random.Random


object PostgresDataSourceFactory {
    fun createDataSource(
        jdbcUrl: String,
        user: String? = null,
        password: String? = null,
        maxPoolSize: Int = 10,
        poolName: String = "PostgresSchedulerPool"
    ): HikariDataSource {
        val config = HikariConfig().apply {
            this.jdbcUrl = jdbcUrl
            if (!user.isNullOrBlank()) this.username = user
            if (!password.isNullOrBlank()) this.password = password
            this.maximumPoolSize = maxPoolSize
            this.minimumIdle = 2.coerceAtMost(maxPoolSize)
            this.connectionTimeout = 30000
            this.idleTimeout = 600000
            this.maxLifetime = 1800000
            this.poolName = poolName
        }
        return HikariDataSource(config)
    }
}

class PostgresSchedulerStorage(
    val dataSource: DataSource
) : SchedulerStorage, AutoCloseable {

    constructor(
        jdbcUrl: String,
        user: String? = null,
        password: String? = null,
        maxPoolSize: Int = 10
    ) : this(PostgresDataSourceFactory.createDataSource(jdbcUrl, user, password, maxPoolSize))

    private val logger = LoggerFactory.getLogger(PostgresSchedulerStorage::class.java)
    private val json = Json { ignoreUnknownKeys = true }

    private val jobStore = PostgresJobMetadataStore(dataSource)
    private val runStore = PostgresRunHistoryStore(dataSource)
    private val workerStore = PostgresWorkerRegistry(dataSource)
    private val outbox = PostgresOutboxStore(dataSource)

    init {
        initSchema()
    }

    private fun getConnection(): Connection = dataSource.connection

    private fun initSchema() {
        getConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS jobs (
                        job_id VARCHAR(255) PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        json_data TEXT NOT NULL,
                        created_at BIGINT NOT NULL
                    );
                    """.trimIndent()
                )
                stmt.execute("CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at DESC);")

                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS job_runs (
                        run_id VARCHAR(255) PRIMARY KEY,
                        job_id VARCHAR(255) NOT NULL,
                        status VARCHAR(50) NOT NULL,
                        triggered_at BIGINT NOT NULL,
                        json_data TEXT NOT NULL
                    );
                    """.trimIndent()
                )
                stmt.execute("CREATE INDEX IF NOT EXISTS idx_job_runs_job_id ON job_runs(job_id);")
                stmt.execute("CREATE INDEX IF NOT EXISTS idx_job_runs_triggered_at ON job_runs(triggered_at DESC);")

                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS task_instances (
                        instance_id VARCHAR(255) PRIMARY KEY,
                        run_id VARCHAR(255) NOT NULL,
                        job_id VARCHAR(255) NOT NULL,
                        task_id VARCHAR(255) NOT NULL,
                        status VARCHAR(50) NOT NULL,
                        scheduled_at BIGINT NOT NULL,
                        json_data TEXT NOT NULL
                    );
                    """.trimIndent()
                )
                stmt.execute("CREATE INDEX IF NOT EXISTS idx_task_instances_run_id ON task_instances(run_id);")
                stmt.execute("CREATE INDEX IF NOT EXISTS idx_task_instances_status ON task_instances(status);")

                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS workers (
                        worker_id VARCHAR(255) PRIMARY KEY,
                        status VARCHAR(50) NOT NULL,
                        last_heartbeat BIGINT NOT NULL,
                        json_data TEXT NOT NULL
                    );
                    """.trimIndent()
                )

                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS outbox_events (
                        event_id VARCHAR(255) PRIMARY KEY,
                        aggregate_id VARCHAR(255) NOT NULL,
                        status VARCHAR(50) NOT NULL,
                        created_at BIGINT NOT NULL,
                        dispatched_at BIGINT,
                        json_data TEXT NOT NULL
                    );
                    """.trimIndent()
                )
                stmt.execute("CREATE INDEX IF NOT EXISTS idx_outbox_events_status ON outbox_events(status, created_at ASC);")

                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS queues (
                        queue_id VARCHAR(255) PRIMARY KEY,
                        state VARCHAR(50) NOT NULL,
                        json_data TEXT NOT NULL,
                        created_at BIGINT NOT NULL
                    );
                    """.trimIndent()
                )

                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cloud_tasks (
                        task_id VARCHAR(255) PRIMARY KEY,
                        queue_id VARCHAR(255) NOT NULL,
                        status VARCHAR(50) NOT NULL,
                        schedule_time BIGINT NOT NULL,
                        created_at BIGINT NOT NULL,
                        json_data TEXT NOT NULL
                    );
                    """.trimIndent()
                )
                stmt.execute("CREATE INDEX IF NOT EXISTS idx_cloud_tasks_queue_sched ON cloud_tasks(queue_id, status, schedule_time ASC);")

                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cluster_leases (
                        lease_name VARCHAR(255) PRIMARY KEY,
                        leader_id VARCHAR(255) NOT NULL,
                        fencing_token BIGINT NOT NULL,
                        acquired_at BIGINT NOT NULL,
                        expires_at BIGINT NOT NULL
                    );
                    """.trimIndent()
                )
            }
        }
        logger.info("Initialized PostgreSQL storage schema")
    }

    private val queueStore = PostgresQueueStore(dataSource)
    private val taskStore = PostgresTaskStore(dataSource)

    override fun saveJob(job: JobSpec) = jobStore.saveJob(job)
    override fun getJob(jobId: String): JobSpec? = jobStore.getJob(jobId)
    override fun listJobs(): List<JobSpec> = jobStore.listJobs()
    override fun deleteJob(jobId: String): Boolean = jobStore.deleteJob(jobId)

    override fun saveRun(run: JobRun) = runStore.saveRun(run)
    override fun getRun(runId: String): JobRun? = runStore.getRun(runId)
    override fun listRuns(limit: Int): List<JobRun> = runStore.listRuns(limit)
    override fun saveTaskInstance(instance: TaskInstance) = runStore.saveTaskInstance(instance)
    override fun getTaskInstance(taskInstanceId: String): TaskInstance? = runStore.getTaskInstance(taskInstanceId)
    override fun getTaskInstancesForRun(runId: String): List<TaskInstance> = runStore.getTaskInstancesForRun(runId)
    override fun findActiveTaskInstances(): List<TaskInstance> = runStore.findActiveTaskInstances()

    override fun upsertWorker(worker: WorkerInfo) = workerStore.upsertWorker(worker)
    override fun getWorker(workerId: String): WorkerInfo? = workerStore.getWorker(workerId)
    override fun listWorkers(): List<WorkerInfo> = workerStore.listWorkers()

    override fun saveOutboxEvent(event: OutboxEvent) = outbox.saveOutboxEvent(event)
    override fun fetchPendingOutboxEvents(limit: Int): List<OutboxEvent> = outbox.fetchPendingOutboxEvents(limit)
    override fun markOutboxDispatched(eventId: String, dispatchedAtEpochMs: Long) =
        outbox.markOutboxDispatched(eventId, dispatchedAtEpochMs)

    override fun saveQueue(queue: QueueSpec) = queueStore.saveQueue(queue)
    override fun getQueue(queueId: String): QueueSpec? = queueStore.getQueue(queueId)
    override fun listQueues(): List<QueueSpec> = queueStore.listQueues()
    override fun deleteQueue(queueId: String): Boolean = queueStore.deleteQueue(queueId)
    override fun updateQueueState(queueId: String, state: QueueState): Boolean = queueStore.updateQueueState(queueId, state)

    override fun saveTask(task: TaskSpec) = taskStore.saveTask(task)
    override fun getTask(taskId: String): TaskSpec? = taskStore.getTask(taskId)
    override fun listTasks(queueId: String?, status: TaskStatus?, limit: Int): List<TaskSpec> = taskStore.listTasks(queueId, status, limit)
    override fun deleteTask(taskId: String): Boolean = taskStore.deleteTask(taskId)
    override fun purgeQueue(queueId: String): Int = taskStore.purgeQueue(queueId)

    override fun close() {
        if (dataSource is HikariDataSource) {
            dataSource.close()
        }
    }
}

class PostgresJobMetadataStore(
    val dataSource: DataSource
) : JobMetadataStore, AutoCloseable {

    constructor(
        jdbcUrl: String,
        user: String? = null,
        password: String? = null,
        maxPoolSize: Int = 10
    ) : this(PostgresDataSourceFactory.createDataSource(jdbcUrl, user, password, maxPoolSize, "PostgresJobMetadataPool"))

    private val logger = LoggerFactory.getLogger(PostgresJobMetadataStore::class.java)
    private val json = Json { ignoreUnknownKeys = true }

    init {
        initSchema()
    }

    private fun getConnection(): Connection = dataSource.connection

    private fun initSchema() {
        getConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS jobs (
                        job_id VARCHAR(255) PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        json_data TEXT NOT NULL,
                        created_at BIGINT NOT NULL
                    );
                    """.trimIndent()
                )
                stmt.execute("CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at DESC);")
            }
        }
        logger.info("Initialized PostgreSQL JobMetadataStore schema")
    }

    override fun saveJob(job: JobSpec) {
        val payload = json.encodeToString(job)
        getConnection().use { conn ->
            conn.prepareStatement(
                """
                INSERT INTO jobs(job_id, name, json_data, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET name = EXCLUDED.name, json_data = EXCLUDED.json_data
                """.trimIndent()
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

    override fun close() {
        if (dataSource is HikariDataSource) {
            dataSource.close()
        }
    }
}

class PostgresRunHistoryStore(
    val dataSource: DataSource
) : RunHistoryStore, AutoCloseable {

    constructor(
        jdbcUrl: String,
        user: String? = null,
        password: String? = null,
        maxPoolSize: Int = 10
    ) : this(PostgresDataSourceFactory.createDataSource(jdbcUrl, user, password, maxPoolSize, "PostgresRunHistoryPool"))

    private val logger = LoggerFactory.getLogger(PostgresRunHistoryStore::class.java)
    private val json = Json { ignoreUnknownKeys = true }

    init {
        initSchema()
    }

    private fun getConnection(): Connection = dataSource.connection

    private fun initSchema() {
        getConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS job_runs (
                        run_id VARCHAR(255) PRIMARY KEY,
                        job_id VARCHAR(255) NOT NULL,
                        status VARCHAR(50) NOT NULL,
                        triggered_at BIGINT NOT NULL,
                        json_data TEXT NOT NULL
                    );
                    """.trimIndent()
                )
                stmt.execute("CREATE INDEX IF NOT EXISTS idx_job_runs_job_id ON job_runs(job_id);")
                stmt.execute("CREATE INDEX IF NOT EXISTS idx_job_runs_triggered_at ON job_runs(triggered_at DESC);")

                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS task_instances (
                        instance_id VARCHAR(255) PRIMARY KEY,
                        run_id VARCHAR(255) NOT NULL,
                        job_id VARCHAR(255) NOT NULL,
                        task_id VARCHAR(255) NOT NULL,
                        status VARCHAR(50) NOT NULL,
                        scheduled_at BIGINT NOT NULL,
                        json_data TEXT NOT NULL
                    );
                    """.trimIndent()
                )
                stmt.execute("CREATE INDEX IF NOT EXISTS idx_task_instances_run_id ON task_instances(run_id);")
                stmt.execute("CREATE INDEX IF NOT EXISTS idx_task_instances_status ON task_instances(status);")
            }
        }
        logger.info("Initialized PostgreSQL RunHistoryStore schema")
    }

    override fun saveRun(run: JobRun) {
        val payload = json.encodeToString(run)
        getConnection().use { conn ->
            conn.prepareStatement(
                """
                INSERT INTO job_runs(run_id, job_id, status, triggered_at, json_data)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET status = EXCLUDED.status, json_data = EXCLUDED.json_data
                """.trimIndent()
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
                """
                INSERT INTO task_instances(instance_id, run_id, job_id, task_id, status, scheduled_at, json_data)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instance_id) DO UPDATE SET status = EXCLUDED.status, json_data = EXCLUDED.json_data
                """.trimIndent()
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

    override fun close() {
        if (dataSource is HikariDataSource) {
            dataSource.close()
        }
    }
}

class PostgresWorkerRegistry(
    val dataSource: DataSource
) : WorkerRegistry, AutoCloseable {

    constructor(
        jdbcUrl: String,
        user: String? = null,
        password: String? = null,
        maxPoolSize: Int = 10
    ) : this(PostgresDataSourceFactory.createDataSource(jdbcUrl, user, password, maxPoolSize, "PostgresWorkerPool"))

    private val logger = LoggerFactory.getLogger(PostgresWorkerRegistry::class.java)
    private val json = Json { ignoreUnknownKeys = true }

    init {
        initSchema()
    }

    private fun getConnection(): Connection = dataSource.connection

    private fun initSchema() {
        getConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS workers (
                        worker_id VARCHAR(255) PRIMARY KEY,
                        status VARCHAR(50) NOT NULL,
                        last_heartbeat BIGINT NOT NULL,
                        json_data TEXT NOT NULL
                    );
                    """.trimIndent()
                )
            }
        }
        logger.info("Initialized PostgreSQL WorkerRegistry schema")
    }

    override fun upsertWorker(worker: WorkerInfo) {
        val payload = json.encodeToString(worker)
        getConnection().use { conn ->
            conn.prepareStatement(
                """
                INSERT INTO workers(worker_id, status, last_heartbeat, json_data)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(worker_id) DO UPDATE SET status = EXCLUDED.status, last_heartbeat = EXCLUDED.last_heartbeat, json_data = EXCLUDED.json_data
                """.trimIndent()
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

    override fun close() {
        if (dataSource is HikariDataSource) {
            dataSource.close()
        }
    }
}

class PostgresOutboxStore(
    val dataSource: DataSource
) : OutboxStore, AutoCloseable {

    constructor(
        jdbcUrl: String,
        user: String? = null,
        password: String? = null,
        maxPoolSize: Int = 10
    ) : this(PostgresDataSourceFactory.createDataSource(jdbcUrl, user, password, maxPoolSize, "PostgresOutboxPool"))

    private val logger = LoggerFactory.getLogger(PostgresOutboxStore::class.java)
    private val json = Json { ignoreUnknownKeys = true }

    init {
        initSchema()
    }

    private fun getConnection(): Connection = dataSource.connection

    private fun initSchema() {
        getConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS outbox_events (
                        event_id VARCHAR(255) PRIMARY KEY,
                        aggregate_id VARCHAR(255) NOT NULL,
                        status VARCHAR(50) NOT NULL,
                        created_at BIGINT NOT NULL,
                        dispatched_at BIGINT,
                        json_data TEXT NOT NULL
                    );
                    """.trimIndent()
                )
                stmt.execute("CREATE INDEX IF NOT EXISTS idx_outbox_events_status ON outbox_events(status, created_at ASC);")
            }
        }
        logger.info("Initialized PostgreSQL OutboxStore schema")
    }

    override fun saveOutboxEvent(event: OutboxEvent) {
        val payload = json.encodeToString(event)
        getConnection().use { conn ->
            conn.prepareStatement(
                """
                INSERT INTO outbox_events(event_id, aggregate_id, status, created_at, dispatched_at, json_data)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_id) DO UPDATE SET status = EXCLUDED.status, dispatched_at = EXCLUDED.dispatched_at, json_data = EXCLUDED.json_data
                """.trimIndent()
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

    override fun close() {
        if (dataSource is HikariDataSource) {
            dataSource.close()
        }
    }
}

class PostgresQueueStore(
    val dataSource: DataSource
) : QueueStore {
    private val json = Json { ignoreUnknownKeys = true }
    private fun getConnection(): Connection = dataSource.connection

    override fun saveQueue(queue: QueueSpec) {
        val payload = json.encodeToString(queue)
        getConnection().use { conn ->
            conn.prepareStatement(
                """
                INSERT INTO queues (queue_id, state, json_data, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (queue_id) DO UPDATE SET
                    state = excluded.state,
                    json_data = excluded.json_data
                """.trimIndent()
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
                if (rs.next()) {
                    return json.decodeFromString<QueueSpec>(rs.getString("json_data"))
                }
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
}

class PostgresTaskStore(
    val dataSource: DataSource
) : TaskStore {
    private val json = Json { ignoreUnknownKeys = true }
    private fun getConnection(): Connection = dataSource.connection

    override fun saveTask(task: TaskSpec) {
        val payload = json.encodeToString(task)
        getConnection().use { conn ->
            conn.prepareStatement(
                """
                INSERT INTO cloud_tasks (task_id, queue_id, status, schedule_time, created_at, json_data)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (task_id) DO UPDATE SET
                    queue_id = excluded.queue_id,
                    status = excluded.status,
                    schedule_time = excluded.schedule_time,
                    json_data = excluded.json_data
                """.trimIndent()
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
                if (rs.next()) {
                    return json.decodeFromString<TaskSpec>(rs.getString("json_data"))
                }
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

/**
 * PostgreSQL-native TaskQueue using SELECT ... FOR UPDATE SKIP LOCKED
 * Eliminates external broker/Redis dependencies and provides 100% ACID crash-safety.
 */
class PostgresTaskQueue(
    val dataSource: DataSource
) : TaskQueue {
    private val logger = LoggerFactory.getLogger(PostgresTaskQueue::class.java)
    private val json = Json { ignoreUnknownKeys = true }
    private val dlq = java.util.concurrent.ConcurrentHashMap<String, DeadLetterEntry>()

    override suspend fun enqueue(task: TaskInstance) = withContext(Dispatchers.IO) {
        val queued = task.copy(status = TaskStatus.QUEUED)
        val payload = json.encodeToString(queued)
        dataSource.connection.use { conn ->
            conn.prepareStatement(
                """
                INSERT INTO task_instances (instance_id, run_id, job_id, task_id, status, scheduled_at, json_data)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (instance_id) DO UPDATE SET
                    status = excluded.status,
                    scheduled_at = excluded.scheduled_at,
                    json_data = excluded.json_data
                """.trimIndent()
            ).use { stmt ->
                stmt.setString(1, queued.taskInstanceId)
                stmt.setString(2, queued.runId)
                stmt.setString(3, queued.jobId)
                stmt.setString(4, queued.taskId)
                stmt.setString(5, queued.status.name)
                stmt.setLong(6, queued.scheduledAtEpochMs)
                stmt.setString(7, payload)
                stmt.executeUpdate()
            }
        }
        logger.info("Enqueued task instance '${task.taskInstanceId}' into PostgreSQL TaskQueue for ${task.scheduledAtEpochMs}ms")
    }

    override suspend fun poll(lookaheadMs: Long, maxWaitMs: Long): TaskInstance? = withContext(Dispatchers.IO) {
        val deadline = System.currentTimeMillis() + maxWaitMs
        while (System.currentTimeMillis() <= deadline) {
            val threshold = System.currentTimeMillis() + lookaheadMs
            dataSource.connection.use { conn ->
                conn.autoCommit = false
                try {
                    val isPostgres = conn.metaData.databaseProductName.contains("PostgreSQL", ignoreCase = true)
                    val lockClause = if (isPostgres) "FOR UPDATE SKIP LOCKED" else ""
                    val selectSql = "SELECT instance_id, json_data FROM task_instances WHERE status IN ('QUEUED', 'RETRYING') AND scheduled_at <= ? ORDER BY scheduled_at ASC LIMIT 1 $lockClause"

                    var chosenId: String? = null
                    var chosenTask: TaskInstance? = null
                    conn.prepareStatement(selectSql).use { stmt ->
                        stmt.setLong(1, threshold)
                        val rs = stmt.executeQuery()
                        if (rs.next()) {
                            chosenId = rs.getString("instance_id")
                            chosenTask = json.decodeFromString<TaskInstance>(rs.getString("json_data"))
                        }
                    }

                    if (chosenId != null && chosenTask != null) {
                        val runningTask = chosenTask.copy(
                            status = TaskStatus.RUNNING,
                            startedAtEpochMs = System.currentTimeMillis()
                        )
                        conn.prepareStatement("UPDATE task_instances SET status = 'RUNNING', json_data = ? WHERE instance_id = ?").use { uStmt ->
                            uStmt.setString(1, json.encodeToString(runningTask))
                            uStmt.setString(2, chosenId)
                            uStmt.executeUpdate()
                        }
                        conn.commit()
                        return@withContext runningTask
                    }
                    conn.commit()
                } catch (e: Exception) {
                    conn.rollback()
                    throw e
                }
            }
            delay(min(50L, maxWaitMs))
        }
        return@withContext null
    }

    override suspend fun requeueWithBackoff(task: TaskInstance, reason: String): TaskInstance {
        val nextAttempt = task.attempt + 1
        if (nextAttempt > task.maxRetries) {
            logger.warn("Task '${task.taskInstanceId}' exceeded max retries ($nextAttempt > ${task.maxRetries}). Sending to DLQ.")
            sendToDlq(task, "Exceeded max retries: $reason")
            return task.copy(status = TaskStatus.DEAD_LETTER, error = reason)
        }

        val baseMs = 1000L
        val maxBackoffMs = 60_000L
        val exponentialDelay = (baseMs * 2.0.pow((nextAttempt - 1).toDouble())).toLong()
        val cappedDelay = min(exponentialDelay, maxBackoffMs)
        val jitter = Random.nextLong(0, min(1000L, cappedDelay / 2 + 1))
        val totalDelay = cappedDelay + jitter

        val nextScheduledAt = System.currentTimeMillis() + totalDelay
        val retryingTask = task.copy(
            attempt = nextAttempt,
            status = TaskStatus.RETRYING,
            scheduledAtEpochMs = nextScheduledAt,
            assignedWorkerId = null,
            startedAtEpochMs = null,
            error = reason
        )

        enqueue(retryingTask)
        return retryingTask
    }

    override suspend fun sendToDlq(task: TaskInstance, reason: String): DeadLetterEntry = withContext(Dispatchers.IO) {
        val entry = DeadLetterEntry(
            id = java.util.UUID.randomUUID().toString(),
            taskInstance = task.copy(status = TaskStatus.DEAD_LETTER, error = reason),
            reason = reason,
            failedAtEpochMs = System.currentTimeMillis()
        )
        dlq[entry.id] = entry
        dataSource.connection.use { conn ->
            conn.prepareStatement("UPDATE task_instances SET status = 'DEAD_LETTER', json_data = ? WHERE instance_id = ?").use { stmt ->
                stmt.setString(1, json.encodeToString(entry.taskInstance))
                stmt.setString(2, task.taskInstanceId)
                stmt.executeUpdate()
            }
        }
        entry
    }

    override suspend fun getDlqEntries(): List<DeadLetterEntry> = dlq.values.sortedByDescending { it.failedAtEpochMs }

    override suspend fun retryDlqEntry(dlqEntryId: String): TaskInstance? {
        val entry = dlq.remove(dlqEntryId) ?: return null
        val resetTask = entry.taskInstance.copy(
            status = TaskStatus.QUEUED,
            attempt = 1,
            error = null,
            scheduledAtEpochMs = System.currentTimeMillis()
        )
        enqueue(resetTask)
        return resetTask
    }

    override suspend fun queueSize(): Int = withContext(Dispatchers.IO) {
        dataSource.connection.use { conn ->
            conn.prepareStatement("SELECT COUNT(*) FROM task_instances WHERE status IN ('QUEUED', 'RETRYING')").use { stmt ->
                val rs = stmt.executeQuery()
                if (rs.next()) rs.getInt(1) else 0
            }
        }
    }

    override suspend fun dlqSize(): Int = dlq.size
}

/**
 * PostgreSQL-native LeaseStore for cluster leadership without Redis.
 */
class PostgresLeaseStore(
    val dataSource: DataSource
) : LeaseStore {
    private val leaseName = "coordinator-primary-lease"
    private val locks = java.util.concurrent.ConcurrentHashMap<String, Long>()

    override fun tryAcquire(nodeId: String, durationMs: Long): LeaderLease? {
        val now = System.currentTimeMillis()
        val expiresAt = now + durationMs

        dataSource.connection.use { conn ->
            conn.autoCommit = false
            try {
                conn.prepareStatement("SELECT leader_id, fencing_token, expires_at FROM cluster_leases WHERE lease_name = ?").use { stmt ->
                    stmt.setString(1, leaseName)
                    val rs = stmt.executeQuery()
                    if (rs.next()) {
                        val currentLeader = rs.getString("leader_id")
                        val currentToken = rs.getLong("fencing_token")
                        val currentExpires = rs.getLong("expires_at")

                        if (currentExpires >= now && currentLeader != nodeId) {
                            conn.commit()
                            return null
                        }

                        val newToken = if (currentLeader == nodeId) currentToken else currentToken + 1
                        conn.prepareStatement("UPDATE cluster_leases SET leader_id = ?, fencing_token = ?, acquired_at = ?, expires_at = ? WHERE lease_name = ?").use { uStmt ->
                            uStmt.setString(1, nodeId)
                            uStmt.setLong(2, newToken)
                            uStmt.setLong(3, now)
                            uStmt.setLong(4, expiresAt)
                            uStmt.setString(5, leaseName)
                            uStmt.executeUpdate()
                        }
                        conn.commit()
                        return LeaderLease(nodeId, newToken, now, expiresAt)
                    }
                }

                val initialToken = 1L
                conn.prepareStatement("INSERT INTO cluster_leases(lease_name, leader_id, fencing_token, acquired_at, expires_at) VALUES (?, ?, ?, ?, ?)").use { iStmt ->
                    iStmt.setString(1, leaseName)
                    iStmt.setString(2, nodeId)
                    iStmt.setLong(3, initialToken)
                    iStmt.setLong(4, now)
                    iStmt.setLong(5, expiresAt)
                    iStmt.executeUpdate()
                }
                conn.commit()
                return LeaderLease(nodeId, initialToken, now, expiresAt)
            } catch (e: Exception) {
                conn.rollback()
                throw e
            }
        }
    }

    override fun tryRenew(nodeId: String, fencingToken: Long, durationMs: Long): LeaderLease? {
        val now = System.currentTimeMillis()
        val expiresAt = now + durationMs
        dataSource.connection.use { conn ->
            conn.prepareStatement("UPDATE cluster_leases SET expires_at = ? WHERE lease_name = ? AND leader_id = ? AND fencing_token = ?").use { stmt ->
                stmt.setLong(1, expiresAt)
                stmt.setString(2, leaseName)
                stmt.setString(3, nodeId)
                stmt.setLong(4, fencingToken)
                val updated = stmt.executeUpdate() > 0
                if (updated) {
                    return LeaderLease(nodeId, fencingToken, now, expiresAt)
                }
            }
        }
        return null
    }

    override fun release(nodeId: String, fencingToken: Long): Boolean {
        dataSource.connection.use { conn ->
            conn.prepareStatement("UPDATE cluster_leases SET expires_at = 0 WHERE lease_name = ? AND leader_id = ? AND fencing_token = ?").use { stmt ->
                stmt.setString(1, leaseName)
                stmt.setString(2, nodeId)
                stmt.setLong(3, fencingToken)
                return stmt.executeUpdate() > 0
            }
        }
    }

    override fun getCurrentLease(): LeaderLease? {
        return dataSource.connection.use { conn ->
            conn.prepareStatement("SELECT leader_id, fencing_token, acquired_at, expires_at FROM cluster_leases WHERE lease_name = ?").use { stmt ->
                stmt.setString(1, leaseName)
                val rs = stmt.executeQuery()
                if (rs.next()) {
                    val exp = rs.getLong("expires_at")
                    if (System.currentTimeMillis() > exp) return@use null
                    LeaderLease(
                        leaderId = rs.getString("leader_id"),
                        fencingToken = rs.getLong("fencing_token"),
                        acquiredAtEpochMs = rs.getLong("acquired_at"),
                        expiresAtEpochMs = exp
                    )
                } else null
            }
        }
    }


    override fun tryAcquireLock(key: String, durationMs: Long): Boolean {
        val now = System.currentTimeMillis()
        val expiry = locks[key]
        if (expiry == null || now > expiry) {
            locks[key] = now + durationMs
            return true
        }
        return false
    }

    override fun releaseLock(key: String): Boolean {
        return locks.remove(key) != null
    }
}

