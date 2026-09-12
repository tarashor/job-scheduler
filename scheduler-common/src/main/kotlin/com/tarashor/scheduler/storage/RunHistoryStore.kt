package com.tarashor.scheduler.storage

import com.tarashor.scheduler.core.model.JobRun
import com.tarashor.scheduler.core.model.TaskInstance

/**
 * Domain Port for managing persistent execution runs and individual task instances.
 * Owned by run history & audit microservice in a Database-per-Microservice architecture.
 */
interface RunHistoryStore {
    fun saveRun(run: JobRun)
    fun getRun(runId: String): JobRun?
    fun listRuns(limit: Int = 100): List<JobRun>

    fun saveTaskInstance(instance: TaskInstance)
    fun getTaskInstance(taskInstanceId: String): TaskInstance?
    fun getTaskInstancesForRun(runId: String): List<TaskInstance>
    fun findActiveTaskInstances(): List<TaskInstance>
}
