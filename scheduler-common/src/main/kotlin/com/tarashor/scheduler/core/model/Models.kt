package com.tarashor.scheduler.core.model

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonNames

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
sealed interface JobAction {
    @Serializable
    @SerialName("Shell")
    data class Shell(val command: String) : JobAction, TaskAction

    @Serializable
    @SerialName("Http")
    data class Http(
        val url: String,
        val method: String = "GET",
        val body: String? = null,
        val headers: Map<String, String> = emptyMap()
    ) : JobAction, TaskAction

    @Serializable
    @SerialName("Simulate")
    data class Simulate(
        val durationMs: Long = 500,
        val shouldFail: Boolean = false,
        val message: String = "Execution successful"
    ) : JobAction, TaskAction
}

// Alias for backwards compatibility
@Serializable
sealed interface TaskAction {
    typealias Shell = JobAction.Shell
    typealias Http = JobAction.Http
    typealias Simulate = JobAction.Simulate
}

@Serializable
data class JobSpec(
    val jobId: String,
    val name: String,
    val schedule: ScheduleSpec = ScheduleSpec.Immediate,
    val action: JobAction = JobAction.Shell("echo 'Job executed'"),
    val timeoutMs: Long = 30_000,
    val maxRetries: Int = 3,
    val backoffBaseMs: Long = 1_000,
    val enabled: Boolean = true,
    val createdAtEpochMs: Long = System.currentTimeMillis()
)

@Serializable
enum class JobStatus {
    SCHEDULED,
    QUEUED,
    RUNNING,
    COMPLETED,
    FAILED,
    RETRYING,
    DEAD_LETTER,
    CANCELLED
}

// Alias for backwards compatibility
typealias TaskStatus = JobStatus

@Serializable
data class JobRun(
    val runId: String,
    val jobId: String,
    val status: JobStatus = JobStatus.QUEUED,
    val triggeredAtEpochMs: Long = System.currentTimeMillis(),
    val startedAtEpochMs: Long? = null,
    val completedAtEpochMs: Long? = null,
    val triggerSource: String = "SCHEDULED",
    val error: String? = null
)

@Serializable
data class JobExecution(
    val executionId: String,
    val jobId: String,
    val runId: String = executionId,
    val status: JobStatus = JobStatus.QUEUED,
    val attempt: Int = 1,
    val maxRetries: Int = 3,
    val action: JobAction,
    val scheduledAtEpochMs: Long = System.currentTimeMillis(),
    val startedAtEpochMs: Long? = null,
    val completedAtEpochMs: Long? = null,
    val assignedWorkerId: String? = null,
    val fencingToken: Long = 0,
    val output: String? = null,
    val error: String? = null,
    val triggerSource: String = "SCHEDULED",
    val lastHeartbeatEpochMs: Long? = null
) {
    // Compatibility accessors
    val taskInstanceId: String get() = executionId
    val taskId: String get() = jobId

    constructor(
        taskInstanceId: String,
        runId: String = taskInstanceId,
        jobId: String,
        taskId: String = jobId,
        status: JobStatus = JobStatus.QUEUED,
        attempt: Int = 1,
        maxRetries: Int = 3,
        action: JobAction,
        scheduledAtEpochMs: Long = System.currentTimeMillis(),
        startedAtEpochMs: Long? = null,
        completedAtEpochMs: Long? = null,
        assignedWorkerId: String? = null,
        fencingToken: Long = 0,
        output: String? = null,
        error: String? = null,
        triggerSource: String = "SCHEDULED",
        lastHeartbeatEpochMs: Long? = null
    ) : this(
        executionId = taskInstanceId,
        jobId = jobId,
        runId = runId,
        status = status,
        attempt = attempt,
        maxRetries = maxRetries,
        action = action,
        scheduledAtEpochMs = scheduledAtEpochMs,
        startedAtEpochMs = startedAtEpochMs,
        completedAtEpochMs = completedAtEpochMs,
        assignedWorkerId = assignedWorkerId,
        fencingToken = fencingToken,
        output = output,
        error = error,
        triggerSource = triggerSource,
        lastHeartbeatEpochMs = lastHeartbeatEpochMs
    )
}

// Alias for backwards compatibility
typealias TaskInstance = JobExecution

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
) {
    val activeExecutionIds: Set<String> get() = activeTaskIds
}

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
    val taskInstance: JobExecution,
    val reason: String,
    val failedAtEpochMs: Long = System.currentTimeMillis()
) {
    val jobExecution: JobExecution get() = taskInstance
}

@Serializable
data class JobExecutionResult(
    val taskInstanceId: String,
    val success: Boolean,
    val output: String? = null,
    val error: String? = null
) {
    val executionId: String get() = taskInstanceId
}

// Alias for backwards compatibility
typealias TaskExecutionResult = JobExecutionResult

@Serializable
data class HeartbeatRequest(
    val workerId: String,
    val currentLoad: Int,
    val activeTaskIds: Set<String> = emptySet()
) {
    val activeExecutionIds: Set<String> get() = activeTaskIds
}

@Serializable
data class HeartbeatResponse(
    val acknowledged: Boolean,
    val leaderId: String,
    val fencingToken: Long
)

@Serializable
enum class OutboxStatus {
    PENDING,
    DISPATCHED,
    FAILED
}

@Serializable
data class OutboxEvent(
    val eventId: String,
    val aggregateType: String = "JOB_EXECUTION",
    val aggregateId: String,
    @OptIn(ExperimentalSerializationApi::class)
    @JsonNames("taskInstance")
    val jobExecution: JobExecution,
    val status: OutboxStatus = OutboxStatus.PENDING,
    val createdAtEpochMs: Long = System.currentTimeMillis(),
    val dispatchedAtEpochMs: Long? = null
) {
    val taskInstance: JobExecution get() = jobExecution

    constructor(
        eventId: String,
        aggregateType: String = "JOB_EXECUTION",
        aggregateId: String,
        taskInstance: JobExecution,
        status: OutboxStatus = OutboxStatus.PENDING,
        createdAtEpochMs: Long = System.currentTimeMillis(),
        dispatchedAtEpochMs: Long? = null,
        @Suppress("UNUSED_PARAMETER") dummy: Unit = Unit
    ) : this(
        eventId = eventId,
        aggregateType = aggregateType,
        aggregateId = aggregateId,
        jobExecution = taskInstance,
        status = status,
        createdAtEpochMs = createdAtEpochMs,
        dispatchedAtEpochMs = dispatchedAtEpochMs
    )
}

// =======================================================
// Google Cloud Tasks Architecture Domain Entities
// ==========================================================

@Serializable
enum class QueueState {
    RUNNING,
    PAUSED,
    DISABLED
}

@Serializable
data class RateLimits(
    val maxDispatchesPerSecond: Double = 50.0,
    val maxConcurrentDispatches: Int = 10,
    val maxBurstSize: Int = 100
)

@Serializable
data class RetryConfig(
    val maxAttempts: Int = 5,
    val minBackoffMs: Long = 1_000L,
    val maxBackoffMs: Long = 300_000L,
    val maxDoublings: Int = 4,
    val maxRetryDurationMs: Long = 86_400_000L
)

@Serializable
data class QueueSpec(
    val queueId: String,
    val state: QueueState = QueueState.RUNNING,
    val rateLimits: RateLimits = RateLimits(),
    val retryConfig: RetryConfig = RetryConfig(),
    val createdAtEpochMs: Long = System.currentTimeMillis()
)

@Serializable
data class QueueStats(
    val queueId: String,
    val state: QueueState,
    val pendingTaskCount: Int = 0,
    val runningTaskCount: Int = 0,
    val completedTaskCount: Int = 0,
    val failedTaskCount: Int = 0,
    val rateLimits: RateLimits = RateLimits(),
    val retryConfig: RetryConfig = RetryConfig()
)

@Serializable
sealed interface TaskTarget {
    @Serializable
    @SerialName("HttpRequest")
    data class HttpRequest(
        val url: String,
        val httpMethod: String = "POST",
        val body: String? = null,
        val headers: Map<String, String> = emptyMap()
    ) : TaskTarget

    @Serializable
    @SerialName("Shell")
    data class Shell(val command: String) : TaskTarget

    @Serializable
    @SerialName("Simulate")
    data class Simulate(
        val durationMs: Long = 500,
        val shouldFail: Boolean = false,
        val message: String = "Execution successful"
    ) : TaskTarget
}

fun TaskTarget.toJobAction(): JobAction = when (this) {
    is TaskTarget.HttpRequest -> JobAction.Http(url, httpMethod, body, headers)
    is TaskTarget.Shell -> JobAction.Shell(command)
    is TaskTarget.Simulate -> JobAction.Simulate(durationMs, shouldFail, message)
}

fun JobAction.toTaskTarget(): TaskTarget = when (this) {
    is JobAction.Http -> TaskTarget.HttpRequest(url, method, body, headers)
    is JobAction.Shell -> TaskTarget.Shell(command)
    is JobAction.Simulate -> TaskTarget.Simulate(durationMs, shouldFail, message)
}

@Serializable
data class TaskSpec(
    val taskId: String,
    val queueId: String = "default",
    val scheduleTimeEpochMs: Long = System.currentTimeMillis(),
    val target: TaskTarget = TaskTarget.HttpRequest(url = "http://localhost:8080/api/mock/target"),
    val status: TaskStatus = TaskStatus.QUEUED,
    val attempt: Int = 0,
    val maxAttempts: Int = 5,
    val createdAtEpochMs: Long = System.currentTimeMillis(),
    val scheduledAtEpochMs: Long = scheduleTimeEpochMs,
    val dispatchedAtEpochMs: Long? = null,
    val completedAtEpochMs: Long? = null,
    val assignedWorkerId: String? = null,
    val responseCode: Int? = null,
    val responseOutput: String? = null,
    val lastError: String? = null
) {
    // Interoperability with JobExecution/TaskInstance
    val taskInstanceId: String get() = taskId
    val executionId: String get() = taskId
    val jobId: String get() = queueId
    val action: JobAction get() = target.toJobAction()

    fun toJobExecution(fencingToken: Long = 0L): JobExecution = JobExecution(
        executionId = taskId,
        jobId = queueId,
        runId = taskId,
        status = status,
        attempt = if (attempt == 0) 1 else attempt,
        maxRetries = maxAttempts,
        action = action,
        scheduledAtEpochMs = scheduledAtEpochMs,
        startedAtEpochMs = dispatchedAtEpochMs,
        completedAtEpochMs = completedAtEpochMs,
        assignedWorkerId = assignedWorkerId,
        fencingToken = fencingToken,
        output = responseOutput,
        error = lastError,
        triggerSource = "CLOUD_TASKS"
    )
}

typealias CloudTask = TaskSpec
typealias CloudTaskQueue = QueueSpec

