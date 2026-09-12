package com.tarashor.scheduler.coordinator

import com.tarashor.scheduler.cluster.LeaseStore
import com.tarashor.scheduler.queue.TaskQueue
import com.tarashor.scheduler.storage.*
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.UUID

@Configuration
class CoordinatorConfig {

    private val storageBundle: StorageBundle by lazy {
        StorageFactory.createCoordinatorStorageFromEnv()
    }

    @Bean
    fun leaseStore(): LeaseStore = storageBundle.leaseStore

    @Bean
    fun jobMetadataStore(): JobMetadataStore = storageBundle.jobMetadataStore

    @Bean
    fun runHistoryStore(): RunHistoryStore = storageBundle.runHistoryStore

    @Bean
    fun workerRegistry(): WorkerRegistry = storageBundle.workerRegistry

    @Bean
    fun taskQueue(): TaskQueue = storageBundle.taskQueue

    @Bean
    fun schedulerCoordinator(
        @Value("\${COORDINATOR_ID:}") envCoordinatorId: String,
        leaseStore: LeaseStore,
        jobMetadataStore: JobMetadataStore,
        runHistoryStore: RunHistoryStore,
        workerRegistry: WorkerRegistry,
        taskQueue: TaskQueue
    ): SchedulerCoordinator {
        val coordinatorId = if (envCoordinatorId.isNotBlank()) {
            envCoordinatorId
        } else {
            System.getenv("COORDINATOR_ID") ?: "coordinator-${UUID.randomUUID().toString().substring(0, 6)}"
        }
        return SchedulerCoordinator(
            coordinatorId = coordinatorId,
            leaseStore = leaseStore,
            jobMetadataStore = jobMetadataStore,
            runHistoryStore = runHistoryStore,
            workerRegistry = workerRegistry,
            taskQueue = taskQueue
        )
    }
}
