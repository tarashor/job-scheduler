package com.tarashor.scheduler.worker

import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Service

@Service
class WorkerService(
    private val workerNode: WorkerNode
) {
    private val logger = LoggerFactory.getLogger(WorkerService::class.java)

    @EventListener(ApplicationReadyEvent::class)
    fun onApplicationReady() {
        logger.info("Starting Worker Node [${workerNode.workerId}] with capacity ${workerNode.capacity}...")
        workerNode.start()
        logger.info("Worker Node [${workerNode.workerId}] is running and listening for tasks.")
    }

    @PreDestroy
    fun onShutdown() {
        logger.info("Shutting down Worker Node [${workerNode.workerId}]...")
        workerNode.stop()
    }
}
