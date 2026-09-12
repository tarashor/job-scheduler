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
    val taskQueue: TaskQueue
)

object StorageFactory {
    private val logger = LoggerFactory.getLogger(StorageFactory::class.java)

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
                taskQueue = redisStorage
            )
        }

        val sqliteDb = System.getenv("SQLITE_DB")
        if (!sqliteDb.isNullOrBlank()) {
            logger.info("Initializing SQLite storage at $sqliteDb")
            return StorageBundle(
                storage = SqliteSchedulerStorage(sqliteDb),
                leaseStore = InMemoryLeaseStore(),
                taskQueue = InMemoryTaskQueue()
            )
        }

        logger.info("Initializing in-memory storage (default)")
        return StorageBundle(
            storage = InMemorySchedulerStorage(),
            leaseStore = InMemoryLeaseStore(),
            taskQueue = InMemoryTaskQueue()
        )
    }
}
