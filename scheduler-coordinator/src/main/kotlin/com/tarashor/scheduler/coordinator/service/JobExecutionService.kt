package com.tarashor.scheduler.coordinator.service

import com.tarashor.scheduler.core.model.TaskExecutionResult
import com.tarashor.scheduler.core.model.TaskInstance
import com.tarashor.scheduler.service.DefaultExecutionCompletionService
import com.tarashor.scheduler.service.ExecutionCompletionService
import com.tarashor.scheduler.queue.TaskQueue
import com.tarashor.scheduler.storage.RunHistoryStore

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
    private val delegate: ExecutionCompletionService =
        DefaultExecutionCompletionService(runHistoryStore, taskQueue)

    override suspend fun handleExecutionCompletion(taskInstance: TaskInstance, result: TaskExecutionResult) {
        delegate.complete(taskInstance, result)
    }
}
