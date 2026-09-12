package com.tarashor.scheduler.worker

import com.tarashor.scheduler.queue.TaskQueue
import com.tarashor.scheduler.storage.*
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.UUID

@Configuration
class WorkerConfig {

    private val storageBundle: StorageBundle by lazy {
        StorageFactory.createWorkerStorageFromEnv()
    }

    @Bean
    fun taskQueue(): TaskQueue = storageBundle.taskQueue

    @Bean
    fun workerRegistry(): WorkerRegistry = storageBundle.workerRegistry

    @Bean
    fun runHistoryStore(): RunHistoryStore = storageBundle.runHistoryStore

    @Bean
    fun taskRunner(): TaskRunner = DefaultTaskRunner()

    @Bean
    fun workerNode(
        @Value("\${WORKER_ID:}") envWorkerId: String,
        @Value("\${WORKER_CAPACITY:4}") capacity: Int,
        taskQueue: TaskQueue,
        workerRegistry: WorkerRegistry,
        runHistoryStore: RunHistoryStore,
        taskRunner: TaskRunner
    ): WorkerNode {
        val workerId = if (envWorkerId.isNotBlank()) {
            envWorkerId
        } else {
            System.getenv("WORKER_ID") ?: "worker-${UUID.randomUUID().toString().substring(0, 6)}"
        }
        val actualCapacity = System.getenv("WORKER_CAPACITY")?.toIntOrNull() ?: capacity
        return WorkerNode(
            workerId = workerId,
            capacity = actualCapacity,
            taskQueue = taskQueue,
            workerRegistry = workerRegistry,
            runHistoryStore = runHistoryStore,
            taskRunner = taskRunner
        )
    }
}
