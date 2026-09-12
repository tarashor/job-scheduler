package com.tarashor.scheduler.storage

import com.tarashor.scheduler.core.model.TaskSpec
import com.tarashor.scheduler.core.model.TaskStatus

/**
 * Domain Port for persisting Cloud Tasks.
 */
interface TaskStore {
    fun saveTask(task: TaskSpec)
    fun getTask(taskId: String): TaskSpec?
    fun listTasks(queueId: String? = null, status: TaskStatus? = null, limit: Int = 100): List<TaskSpec>
    fun deleteTask(taskId: String): Boolean
    fun purgeQueue(queueId: String): Int
}
