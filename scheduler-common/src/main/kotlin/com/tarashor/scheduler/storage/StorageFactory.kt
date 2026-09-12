package com.tarashor.scheduler.storage

import com.tarashor.scheduler.cluster.InMemoryLeaseStore
import com.tarashor.scheduler.cluster.LeaseStore
import com.tarashor.scheduler.queue.InMemoryTaskQueue
import com.tarashor.scheduler.queue.TaskQueue
import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisPool

data class StorageBundle(
    val storage: SchedulerStorage,
    val leaseStore: LeaseStore,
    val taskQueue: TaskQueue,
    val jobMetadataStore: JobMetadataStore = storage,
    val runHistoryStore: RunHistoryStore = storage,
    val workerRegistry: WorkerRegistry = storage,
    val outboxStore: OutboxStore = storage,
    val triggerService: com.tarashor.scheduler.service.JobTriggerService = com.tarashor.scheduler.service.DefaultJobTriggerService(
        jobMetadataStore, runHistoryStore, storage, taskQueue
    )
)

object StorageFactory {
    private val logger = LoggerFactory.getLogger(StorageFactory::class.java)

    fun getPostgresJdbcUrl(): String? {
        val direct = System.getenv("POSTGRES_JDBC_URL") ?: System.getenv("POSTGRES_URL")
        if (!direct.isNullOrBlank()) return direct
        val host = System.getenv("POSTGRES_HOST")
        if (!host.isNullOrBlank()) {
            val port = System.getenv("POSTGRES_PORT") ?: "5432"
            val db = System.getenv("POSTGRES_DB") ?: "scheduler"
            return "jdbc:postgresql://$host:$port/$db"
        }
        return null
    }

    /**
     * Creates storage tailored for the scheduler-api microservice.
     * Supports isolated persistent metadata DB (PostgreSQL / SQLite) for Job Specs,
     * while delegating task queue and leases to Redis/In-Memory.
     */
    fun createApiStorageFromEnv(): StorageBundle {
        val baseBundle = createFromEnv()
        val pgUrl = getPostgresJdbcUrl()

        if (!pgUrl.isNullOrBlank()) {
            val pgUser = System.getenv("POSTGRES_USER") ?: "postgres"
            val pgPass = System.getenv("POSTGRES_PASSWORD") ?: "postgres"
            val maxPool = System.getenv("POSTGRES_MAX_POOL_SIZE")?.toIntOrNull() ?: 15
            logger.info("Database-per-Microservice: Using scalable PostgreSQL JobMetadataStore at $pgUrl (poolSize=$maxPool)")

            val ds = PostgresDataSourceFactory.createDataSource(pgUrl, pgUser, pgPass, maxPoolSize = maxPool, poolName = "ApiPostgresPool")
            val jobStore = PostgresJobMetadataStore(ds)
            val runStore = PostgresRunHistoryStore(ds)
            val outboxStore = PostgresOutboxStore(ds)

            val composite = CompositeSchedulerStorage(
                jobMetadataStore = jobStore,
                runHistoryStore = runStore,
                workerRegistry = baseBundle.workerRegistry
            )
            return StorageBundle(
                storage = composite,
                leaseStore = baseBundle.leaseStore,
                taskQueue = baseBundle.taskQueue,
                jobMetadataStore = jobStore,
                runHistoryStore = runStore,
                workerRegistry = baseBundle.workerRegistry,
                outboxStore = outboxStore
            )
        }

        val metadataDbPath = System.getenv("METADATA_SQLITE_PATH")
            ?: System.getenv("METADATA_DB_PATH")

        if (!metadataDbPath.isNullOrBlank()) {
            logger.info("Database-per-Microservice: Using dedicated SQLite JobMetadataStore at $metadataDbPath")
            val jobStore = SqliteJobMetadataStore(metadataDbPath)
            val composite = CompositeSchedulerStorage(
                jobMetadataStore = jobStore,
                runHistoryStore = baseBundle.runHistoryStore,
                workerRegistry = baseBundle.workerRegistry
            )
            return StorageBundle(
                storage = composite,
                leaseStore = baseBundle.leaseStore,
                taskQueue = baseBundle.taskQueue,
                jobMetadataStore = jobStore,
                runHistoryStore = baseBundle.runHistoryStore,
                workerRegistry = baseBundle.workerRegistry
            )
        }

        return baseBundle
    }

    /**
     * Creates storage tailored for the stateless scheduler-worker microservice.
     * Workers do NOT need access to the job metadata store!
     */
    fun createWorkerStorageFromEnv(): StorageBundle {
        return createFromEnv()
    }

    /**
     * Creates storage tailored for the scheduler-coordinator microservice.
     */
    fun createCoordinatorStorageFromEnv(): StorageBundle {
        val baseBundle = createFromEnv()
        val pgUrl = getPostgresJdbcUrl()

        if (!pgUrl.isNullOrBlank()) {
            val pgUser = System.getenv("POSTGRES_USER") ?: "postgres"
            val pgPass = System.getenv("POSTGRES_PASSWORD") ?: "postgres"
            val maxPool = System.getenv("POSTGRES_MAX_POOL_SIZE")?.toIntOrNull() ?: 10
            logger.info("Coordinator: Attaching scalable PostgreSQL JobMetadataStore at $pgUrl")

            val ds = PostgresDataSourceFactory.createDataSource(pgUrl, pgUser, pgPass, maxPoolSize = maxPool, poolName = "CoordinatorPostgresPool")
            val jobStore = PostgresJobMetadataStore(ds)
            val runStore = PostgresRunHistoryStore(ds)
            val outboxStore = PostgresOutboxStore(ds)

            val composite = CompositeSchedulerStorage(
                jobMetadataStore = jobStore,
                runHistoryStore = runStore,
                workerRegistry = baseBundle.workerRegistry
            )
            return StorageBundle(
                storage = composite,
                leaseStore = baseBundle.leaseStore,
                taskQueue = baseBundle.taskQueue,
                jobMetadataStore = jobStore,
                runHistoryStore = runStore,
                workerRegistry = baseBundle.workerRegistry,
                outboxStore = outboxStore
            )
        }

        val metadataDbPath = System.getenv("METADATA_SQLITE_PATH")
            ?: System.getenv("METADATA_DB_PATH")

        if (!metadataDbPath.isNullOrBlank()) {
            logger.info("Coordinator: Attaching dedicated SQLite JobMetadataStore at $metadataDbPath")
            val jobStore = SqliteJobMetadataStore(metadataDbPath)
            val composite = CompositeSchedulerStorage(
                jobMetadataStore = jobStore,
                runHistoryStore = baseBundle.runHistoryStore,
                workerRegistry = baseBundle.workerRegistry
            )
            return StorageBundle(
                storage = composite,
                leaseStore = baseBundle.leaseStore,
                taskQueue = baseBundle.taskQueue,
                jobMetadataStore = jobStore,
                runHistoryStore = baseBundle.runHistoryStore,
                workerRegistry = baseBundle.workerRegistry
            )
        }

        return baseBundle
    }

    fun createFromEnv(): StorageBundle {
        val redisHost = System.getenv("REDIS_HOST")
        if (!redisHost.isNullOrBlank()) {
            val redisPort = System.getenv("REDIS_PORT")?.toIntOrNull() ?: 6379
            logger.info("Initializing Redis storage at $redisHost:$redisPort")
            val pool = JedisPool(redisHost, redisPort)
            val redisStorage = RedisStorage(pool)
            return StorageBundle(
                storage = redisStorage,
                leaseStore = redisStorage,
                taskQueue = redisStorage,
                jobMetadataStore = redisStorage,
                runHistoryStore = redisStorage,
                workerRegistry = redisStorage
            )
        }

        val pgUrl = getPostgresJdbcUrl()
        if (!pgUrl.isNullOrBlank()) {
            val pgUser = System.getenv("POSTGRES_USER") ?: "postgres"
            val pgPass = System.getenv("POSTGRES_PASSWORD") ?: "postgres"
            val maxPool = System.getenv("POSTGRES_MAX_POOL_SIZE")?.toIntOrNull() ?: 15
            logger.info("Initializing full PostgreSQL storage at $pgUrl")
            val pgStorage = PostgresSchedulerStorage(pgUrl, pgUser, pgPass, maxPool)
            return StorageBundle(
                storage = pgStorage,
                leaseStore = InMemoryLeaseStore(),
                taskQueue = InMemoryTaskQueue(),
                jobMetadataStore = pgStorage,
                runHistoryStore = pgStorage,
                workerRegistry = pgStorage,
                outboxStore = pgStorage
            )
        }

        val sqliteDb = System.getenv("SQLITE_DB")
        if (!sqliteDb.isNullOrBlank()) {
            logger.info("Initializing SQLite storage at $sqliteDb")
            val sqliteStorage = SqliteSchedulerStorage(sqliteDb)
            return StorageBundle(
                storage = sqliteStorage,
                leaseStore = InMemoryLeaseStore(),
                taskQueue = InMemoryTaskQueue(),
                jobMetadataStore = sqliteStorage,
                runHistoryStore = sqliteStorage,
                workerRegistry = sqliteStorage
            )
        }

        logger.info("Initializing in-memory storage (default)")
        val inMemory = InMemorySchedulerStorage()
        return StorageBundle(
            storage = inMemory,
            leaseStore = InMemoryLeaseStore(),
            taskQueue = InMemoryTaskQueue(),
            jobMetadataStore = inMemory,
            runHistoryStore = inMemory,
            workerRegistry = inMemory
        )
    }
}
