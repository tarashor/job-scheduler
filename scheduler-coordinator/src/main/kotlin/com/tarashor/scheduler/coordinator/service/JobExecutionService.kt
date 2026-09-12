package com.tarashor.scheduler.coordinator.service

import com.tarashor.scheduler.core.model.JobStatus
import com.tarashor.scheduler.core.model.TaskExecutionResult
import com.tarashor.scheduler.core.model.TaskInstance
import com.tarashor.scheduler.queue.TaskQueue
import com.tarashor.scheduler.storage.RunHistoryStore
import org.slf4j.LoggerFactory

/**
 * Single Responsibility: Processes execution results reported by workers,
 * transitions JobExecution and JobRun states, and triggers retry backoff or DLQ placement.
 */
interface JobExecutionService {
    suspend fun handleExecutionCompletion(taskInstance: TaskInstance, result: TaskExecutionResult)
}

class DefaultJobExecutionService(
    private val runHistoryStore: RunHistoryStore,
    private val taskQueue: TaskQueue
) : JobExecutionService {
    private val logger = LoggerFactory.getLogger(DefaultJobExecutionService::class.java)

    override suspend fun handleExecutionCompletion(taskInstance: TaskInstance, result: TaskExecutionResult) {
        val run = runHistoryStore.getRun(taskInstance.runId)

        if (result.success) {
            val completedInst = taskInstance.copy(
                status = JobStatus.COMPLETED,
                completedAtEpochMs = System.currentTimeMillis(),
                output = result.output
            )
            runHistoryStore.saveTaskInstance(completedInst)
            if (run != null) {
                runHistoryStore.saveRun(
                    run.copy(
                        status = JobStatus.COMPLETED,
                        completedAtEpochMs = System.currentTimeMillis()
                    )
                )
            }
            logger.info("Job '${taskInstance.jobId}' (Execution: '${taskInstance.executionId}') COMPLETED successfully")
        } else {
            logger.warn("Job '${taskInstance.jobId}' (Execution: '${taskInstance.executionId}') failed: ${result.error}")
            val nextAttempt = taskInstance.attempt + 1
            if (nextAttempt <= taskInstance.maxRetries) {
                val retried = taskQueue.requeueWithBackoff(taskInstance, result.error ?: "Execution failed")
                runHistoryStore.saveTaskInstance(retried)
            } else {
                val failedInst = taskInstance.copy(
                    status = JobStatus.DEAD_LETTER,
                    completedAtEpochMs = System.currentTimeMillis(),
                    error = result.error
                )
                runHistoryStore.saveTaskInstance(failedInst)
                if (run != null) {
                    runHistoryStore.saveRun(
                        run.copy(
                            status = JobStatus.FAILED,
                            completedAtEpochMs = System.currentTimeMillis(),
                            error = result.error
                        )
                    )
                }
            }
        }
    }
}
