package com.tarashor.scheduler.core.model

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
sealed interface ScheduleSpec {
    @Serializable
    @SerialName("OneOff")
    data class OneOff(val timestampEpochMs: Long) : ScheduleSpec

    @Serializable
    @SerialName("Cron")
    data class Cron(val cronExpression: String) : ScheduleSpec

    @Serializable
    @SerialName("Interval")
    data class Interval(val intervalMs: Long) : ScheduleSpec

    @Serializable
    @SerialName("Immediate")
    data object Immediate : ScheduleSpec
}

@Serializable
sealed interface TaskAction {
    @Serializable
    @SerialName("Shell")
    data class Shell(val command: String) : TaskAction

    @Serializable
    @SerialName("Http")
    data class Http(
        val url: String,
        val method: String = "GET",
        val body: String? = null,
        val headers: Map<String, String> = emptyMap()
    ) : TaskAction

    @Serializable
    @SerialName("Simulate")
    data class Simulate(
        val durationMs: Long = 500,
        val shouldFail: Boolean = false,
        val message: String = "Execution successful"
    ) : TaskAction
}

@Serializable
data class TaskSpec(
    val taskId: String,
    val name: String,
    val action: TaskAction,
    val timeoutMs: Long = 30_000,
    val maxRetries: Int = 3,
    val backoffBaseMs: Long = 1_000
)

@Serializable
data class JobSpec(
    val jobId: String,
    val name: String,
    val schedule: ScheduleSpec,
    val tasks: List<TaskSpec>,
    val enabled: Boolean = true,
    val createdAtEpochMs: Long = System.currentTimeMillis()
)

@Serializable
enum class JobStatus {
    PENDING,
    RUNNING,
    COMPLETED,
    FAILED,
    CANCELLED
}

@Serializable
enum class TaskStatus {
    QUEUED,
    RUNNING,
    COMPLETED,
    FAILED,
    RETRYING,
    DEAD_LETTER,
    CANCELLED
}

@Serializable
data class JobRun(
    val runId: String,
    val jobId: String,
    val status: JobStatus = JobStatus.PENDING,
    val triggeredAtEpochMs: Long = System.currentTimeMillis(),
    val startedAtEpochMs: Long? = null,
    val completedAtEpochMs: Long? = null,
    val triggerSource: String = "MANUAL",
    val error: String? = null
)

@Serializable
data class TaskInstance(
    val taskInstanceId: String,
    val runId: String,
    val jobId: String,
    val taskId: String,
    val status: TaskStatus = TaskStatus.QUEUED,
    val attempt: Int = 1,
    val maxRetries: Int = 3,
    val action: TaskAction,
    val scheduledAtEpochMs: Long,
    val startedAtEpochMs: Long? = null,
    val completedAtEpochMs: Long? = null,
    val assignedWorkerId: String? = null,
    val fencingToken: Long = 0,
    val output: String? = null,
    val error: String? = null,
    val lastHeartbeatEpochMs: Long? = null
)

@Serializable
enum class WorkerStatus {
    HEALTHY,
    SUSPECT,
    DEAD,
    DRAINING
}

@Serializable
data class WorkerInfo(
    val workerId: String,
    val hostname: String = "localhost",
    val port: Int = 9001,
    val capacity: Int = 4,
    val currentLoad: Int = 0,
    val registeredAtEpochMs: Long = System.currentTimeMillis(),
    val lastHeartbeatEpochMs: Long = System.currentTimeMillis(),
    val status: WorkerStatus = WorkerStatus.HEALTHY,
    val activeTaskIds: Set<String> = emptySet()
)

@Serializable
data class LeaderLease(
    val leaderId: String,
    val fencingToken: Long,
    val acquiredAtEpochMs: Long,
    val expiresAtEpochMs: Long
)

@Serializable
data class DeadLetterEntry(
    val id: String,
    val taskInstance: TaskInstance,
    val reason: String,
    val failedAtEpochMs: Long = System.currentTimeMillis()
)

@Serializable
data class TaskExecutionResult(
    val taskInstanceId: String,
    val success: Boolean,
    val output: String? = null,
    val error: String? = null
)

@Serializable
data class HeartbeatRequest(
    val workerId: String,
    val currentLoad: Int,
    val activeTaskIds: Set<String>
)

@Serializable
data class HeartbeatResponse(
    val acknowledged: Boolean,
    val leaderId: String,
    val fencingToken: Long
)
