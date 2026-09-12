package com.tarashor.scheduler.storage

import com.tarashor.scheduler.core.model.*
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.sql.Connection
import javax.sql.DataSource

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
            }
        }
        logger.info("Initialized PostgreSQL storage schema")
    }

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
