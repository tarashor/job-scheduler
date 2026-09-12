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
    val workerRegistry: WorkerRegistry = storage
)

object StorageFactory {
    private val logger = LoggerFactory.getLogger(StorageFactory::class.java)

    /**
     * Creates storage tailored for the scheduler-api microservice.
     * Supports isolated persistent metadata DB (e.g. SQLite/PostgreSQL) for Job Specs,
     * while delegating task queue and leases to Redis/In-Memory.
     */
    fun createApiStorageFromEnv(): StorageBundle {
        val metadataDbPath = System.getenv("METADATA_SQLITE_PATH")
            ?: System.getenv("METADATA_DB_PATH")

        val baseBundle = createFromEnv()

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
        val metadataDbPath = System.getenv("METADATA_SQLITE_PATH")
            ?: System.getenv("METADATA_DB_PATH")

        val baseBundle = createFromEnv()

        if (!metadataDbPath.isNullOrBlank()) {
            logger.info("Coordinator: Attaching dedicated JobMetadataStore at $metadataDbPath")
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
