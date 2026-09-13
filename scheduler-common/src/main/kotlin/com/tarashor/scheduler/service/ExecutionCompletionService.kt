package com.tarashor.scheduler.service

import com.tarashor.scheduler.core.model.JobStatus
import com.tarashor.scheduler.core.model.TaskExecutionResult
import com.tarashor.scheduler.core.model.TaskInstance
import com.tarashor.scheduler.queue.TaskQueue
import com.tarashor.scheduler.storage.RunHistoryStore
import org.slf4j.LoggerFactory

/**
 * The single owner of execution state transitions after a worker attempt.
 * Both embedded workers and coordinator callbacks use this service so that
 * retries, terminal failures, and run history cannot diverge by code path.
 */
interface ExecutionCompletionService {
    suspend fun complete(taskInstance: TaskInstance, result: TaskExecutionResult)
}

class DefaultExecutionCompletionService(
    private val runHistoryStore: RunHistoryStore,
    private val taskQueue: TaskQueue
) : ExecutionCompletionService {
    private val logger = LoggerFactory.getLogger(DefaultExecutionCompletionService::class.java)

    override suspend fun complete(taskInstance: TaskInstance, result: TaskExecutionResult) {
        val run = runHistoryStore.getRun(taskInstance.runId)

        if (result.success) {
            val completed = taskInstance.copy(
                status = JobStatus.COMPLETED,
                completedAtEpochMs = System.currentTimeMillis(),
                output = result.output,
                error = null
            )
            runHistoryStore.saveTaskInstance(completed)
            if (run != null) {
                runHistoryStore.saveRun(
                    run.copy(
                        status = JobStatus.COMPLETED,
                        completedAtEpochMs = System.currentTimeMillis(),
                        error = null
                    )
                )
            }
            return
        }

        val requeued = taskQueue.requeueWithBackoff(
            taskInstance,
            result.error ?: "Execution failed"
        )
        runHistoryStore.saveTaskInstance(requeued)

        if (requeued.status == JobStatus.DEAD_LETTER) {
            if (run != null) {
                runHistoryStore.saveRun(
                    run.copy(
                        status = JobStatus.FAILED,
                        completedAtEpochMs = System.currentTimeMillis(),
                        error = result.error
                    )
                )
            }
            logger.warn("Execution '${taskInstance.executionId}' moved to DLQ")
        } else {
            logger.info("Execution '${taskInstance.executionId}' requeued as attempt ${requeued.attempt}")
        }
    }
}
