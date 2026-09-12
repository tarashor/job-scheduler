package com.tarashor.scheduler.worker

import com.tarashor.scheduler.storage.StorageFactory
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.util.UUID

fun main() = runBlocking {
    val logger = LoggerFactory.getLogger("WorkerApp")
    val workerId = System.getenv("WORKER_ID") ?: "worker-${UUID.randomUUID().toString().substring(0, 6)}"
    val capacity = System.getenv("WORKER_CAPACITY")?.toIntOrNull() ?: 4
    logger.info("Starting Worker Microservice Pod [$workerId] with capacity $capacity...")

    val bundle = StorageFactory.createFromEnv()
    val worker = WorkerNode(
        workerId = workerId,
        capacity = capacity,
        taskQueue = bundle.taskQueue,
        storage = bundle.storage
    )
    worker.start()

    Runtime.getRuntime().addShutdownHook(Thread {
        logger.info("Shutting down worker pod $workerId...")
        worker.stop()
    })

    logger.info("Worker $workerId is actively listening for tasks.")
    while (true) {
        delay(10_000)
    }
}
