package com.tarashor.scheduler.core.dag

import com.tarashor.scheduler.core.model.JobStatus
import com.tarashor.scheduler.core.model.TaskInstance
import com.tarashor.scheduler.core.model.TaskSpec
import com.tarashor.scheduler.core.model.TaskStatus

class InvalidDAGException(message: String) : IllegalArgumentException(message)

object DAGEngine {

    /**
     * Validates task dependencies and checks for cycles using Kahn's algorithm.
     * Returns the topological execution order of task IDs.
     */
    fun validateAndSort(tasks: List<TaskSpec>): List<String> {
        val taskMap = tasks.associateBy { it.taskId }
        if (taskMap.size != tasks.size) {
            throw InvalidDAGException("Duplicate task IDs found in job definition")
        }

        // Validate all dependencies exist
        for (task in tasks) {
            for (dep in task.dependencies) {
                if (!taskMap.containsKey(dep)) {
                    throw InvalidDAGException("Task '${task.taskId}' depends on unknown task '$dep'")
                }
                if (dep == task.taskId) {
                    throw InvalidDAGException("Task '${task.taskId}' cannot depend on itself")
                }
            }
        }

        // Kahn's algorithm
        val inDegree = mutableMapOf<String, Int>()
        val dependents = mutableMapOf<String, MutableList<String>>()

        for (task in tasks) {
            inDegree[task.taskId] = task.dependencies.size
            for (dep in task.dependencies) {
                dependents.computeIfAbsent(dep) { mutableListOf() }.add(task.taskId)
            }
        }

        val queue = ArrayDeque<String>()
        for ((taskId, degree) in inDegree) {
            if (degree == 0) {
                queue.add(taskId)
            }
        }

        val sorted = mutableListOf<String>()
        while (queue.isNotEmpty()) {
            val current = queue.removeFirst()
            sorted.add(current)

            for (dependent in dependents[current].orEmpty()) {
                val updatedDegree = (inDegree[dependent] ?: 1) - 1
                inDegree[dependent] = updatedDegree
                if (updatedDegree == 0) {
                    queue.add(dependent)
                }
            }
        }

        if (sorted.size != tasks.size) {
            val cyclicTasks = inDegree.filter { it.value > 0 }.keys
            throw InvalidDAGException("Cyclic dependency detected among tasks: $cyclicTasks")
        }

        return sorted
    }

    /**
     * Determines which tasks in the DAG are newly ready for execution.
     * A task is eligible when all of its upstream dependencies are COMPLETED.
     */
    fun findReadyTasks(
        tasks: List<TaskSpec>,
        instances: Map<String, TaskInstance>
    ): List<TaskSpec> {
        val ready = mutableListOf<TaskSpec>()

        for (task in tasks) {
            val instance = instances[task.taskId]
            val isAlreadyHandled = instance != null && instance.status in setOf(
                TaskStatus.READY,
                TaskStatus.QUEUED,
                TaskStatus.RUNNING,
                TaskStatus.COMPLETED,
                TaskStatus.DEAD_LETTER,
                TaskStatus.FAILED,
                TaskStatus.RETRYING
            )

            if (isAlreadyHandled) continue

            val allDependenciesSatisfied = task.dependencies.all { depId ->
                instances[depId]?.status == TaskStatus.COMPLETED
            }

            if (allDependenciesSatisfied) {
                ready.add(task)
            }
        }

        return ready
    }

    /**
     * Evaluates the overall status of the JobRun based on individual task instance states.
     */
    fun evaluateJobStatus(
        tasks: List<TaskSpec>,
        instances: Map<String, TaskInstance>
    ): JobStatus {
        if (tasks.isEmpty()) return JobStatus.COMPLETED

        val allCompleted = tasks.all { instances[it.taskId]?.status == TaskStatus.COMPLETED }
        if (allCompleted) return JobStatus.COMPLETED

        val hasTerminalFailure = tasks.any {
            val inst = instances[it.taskId]
            inst?.status == TaskStatus.DEAD_LETTER || (inst?.status == TaskStatus.FAILED && inst.attempt >= inst.maxRetries)
        }
        if (hasTerminalFailure) return JobStatus.FAILED

        return JobStatus.RUNNING
    }
}
