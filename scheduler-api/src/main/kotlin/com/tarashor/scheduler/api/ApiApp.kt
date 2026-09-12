package com.tarashor.scheduler.api

import com.tarashor.scheduler.storage.StorageFactory
import org.slf4j.LoggerFactory

fun main() {
    val logger = LoggerFactory.getLogger("ApiApp")
    val port = System.getenv("PORT")?.toIntOrNull() ?: 8080
    logger.info("Starting Scheduler API Gateway Microservice on port $port...")

    val bundle = StorageFactory.createFromEnv()
    val server = SchedulerApiServer(
        port = port,
        storage = bundle.storage,
        taskQueue = bundle.taskQueue,
        leaseStore = bundle.leaseStore
    )

    Runtime.getRuntime().addShutdownHook(Thread {
        logger.info("Stopping API server...")
        server.stop()
    })

    server.start(wait = true)
}
