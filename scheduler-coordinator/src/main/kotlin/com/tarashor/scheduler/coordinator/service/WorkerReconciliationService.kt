package com.tarashor.scheduler.coordinator.service

import com.tarashor.scheduler.core.model.WorkerStatus
import com.tarashor.scheduler.queue.TaskQueue
import com.tarashor.scheduler.storage.RunHistoryStore
import com.tarashor.scheduler.storage.WorkerRegistry
import org.slf4j.LoggerFactory

/**
 * Single Responsibility: Detects worker node failures based on heartbeat timeouts
 * and reclaims stranded active executions back to the queue with retry backoff.
 */
interface WorkerReconciliationService {
    suspend fun reapDeadWorkers(workerHeartbeatTimeoutMs: Long = 8_000L): Int
}

class DefaultWorkerReconciliationService(
    private val workerRegistry: WorkerRegistry,
    private val runHistoryStore: RunHistoryStore,
    private val taskQueue: TaskQueue
) : WorkerReconciliationService {
    private val logger = LoggerFactory.getLogger(DefaultWorkerReconciliationService::class.java)

    override suspend fun reapDeadWorkers(workerHeartbeatTimeoutMs: Long): Int {
        val now = System.currentTimeMillis()
        val workers = workerRegistry.listWorkers()
        var reclaimedCount = 0

        for (worker in workers) {
            if (worker.status == WorkerStatus.HEALTHY && (now - worker.lastHeartbeatEpochMs) > workerHeartbeatTimeoutMs) {
                logger.error("Worker '${worker.workerId}' missed heartbeats for ${now - worker.lastHeartbeatEpochMs}ms! Marking DEAD.")
                val deadWorker = worker.copy(status = WorkerStatus.DEAD, currentLoad = 0)
                workerRegistry.upsertWorker(deadWorker)

                // Reclaim all tasks assigned to this dead worker
                val activeTasks = runHistoryStore.findActiveTaskInstances().filter { it.assignedWorkerId == worker.workerId }
                for (task in activeTasks) {
                    logger.warn("Reclaiming task '${task.taskInstanceId}' from dead worker '${worker.workerId}'")
                    val requeued = taskQueue.requeueWithBackoff(task, "Worker '${worker.workerId}' crashed / timed out")
                    runHistoryStore.saveTaskInstance(requeued)
                    reclaimedCount++
                }
            }
        }
        return reclaimedCount
    }
}
