package com.tarashor.scheduler.coordinator

import jakarta.annotation.PreDestroy
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Service

@Service
class CoordinatorService(
    private val coordinator: SchedulerCoordinator
) {
    private val logger = LoggerFactory.getLogger(CoordinatorService::class.java)

    @EventListener(ApplicationReadyEvent::class)
    fun onApplicationReady() {
        logger.info("Starting Scheduler Coordinator [${coordinator.coordinatorId}]...")
        coordinator.start()
        logger.info("Coordinator [${coordinator.coordinatorId}] started successfully and awaiting leadership.")
    }

    @PreDestroy
    fun onShutdown() {
        logger.info("Shutting down Scheduler Coordinator [${coordinator.coordinatorId}]...")
        runBlocking {
            coordinator.stop()
        }
    }
}
