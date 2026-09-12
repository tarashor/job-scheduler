package com.tarashor.scheduler.api.dto

import com.tarashor.scheduler.core.model.JobStatus
import com.tarashor.scheduler.core.model.TaskInstance
import kotlinx.serialization.Serializable

@Serializable
data class ClusterHealthResponse(
    val coordinatorId: String,
    val isLeader: Boolean,
    val fencingToken: Long,
    val queueSize: Int,
    val dlqSize: Int
)

@Serializable
data class EnrichedJobRun(
    val runId: String,
    val jobId: String,
    val status: JobStatus,
    val triggeredAtEpochMs: Long,
    val startedAtEpochMs: Long? = null,
    val completedAtEpochMs: Long? = null,
    val triggerSource: String = "MANUAL",
    val error: String? = null,
    val taskInstances: List<TaskInstance> = emptyList()
)
