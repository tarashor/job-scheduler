package com.tarashor.scheduler.coordinator

import com.tarashor.scheduler.storage.StorageFactory
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.util.UUID

fun main() = runBlocking {
    val logger = LoggerFactory.getLogger("CoordinatorApp")
    val coordinatorId = System.getenv("COORDINATOR_ID") ?: "coordinator-${UUID.randomUUID().toString().substring(0, 6)}"
    logger.info("Starting Scheduler Coordinator Microservice [$coordinatorId]...")

    val bundle = StorageFactory.createCoordinatorStorageFromEnv()
    val coordinator = SchedulerCoordinator(
        coordinatorId = coordinatorId,
        leaseStore = bundle.leaseStore,
        jobMetadataStore = bundle.jobMetadataStore,
        runHistoryStore = bundle.runHistoryStore,
        workerRegistry = bundle.workerRegistry,
        taskQueue = bundle.taskQueue
    )
    coordinator.start()

    Runtime.getRuntime().addShutdownHook(Thread {
        logger.info("Shutting down coordinator $coordinatorId...")
        runBlocking { coordinator.stop() }
    })

    logger.info("Coordinator $coordinatorId is running. Awaiting leader election.")
    while (true) {
        delay(10_000)
    }
}
