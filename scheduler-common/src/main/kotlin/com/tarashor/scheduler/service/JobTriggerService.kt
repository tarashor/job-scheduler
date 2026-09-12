package com.tarashor.scheduler.service

import com.tarashor.scheduler.core.model.*
import com.tarashor.scheduler.queue.TaskQueue
import com.tarashor.scheduler.storage.JobMetadataStore
import com.tarashor.scheduler.storage.OutboxStore
import com.tarashor.scheduler.storage.RunHistoryStore
import org.slf4j.LoggerFactory
import java.util.UUID

/**
 * Application Use Case / Service for triggering atomic jobs.
 * Encapsulates the complete workflow:
 * 1. Validating job exists
 * 2. Calculating scheduled timestamp
 * 3. Creating JobRun and JobExecution records
 * 4. Writing transactional outbox event
 * 5. Fast-path enqueuing into the distributed TaskQueue
 * 6. Marking outbox event dispatched
 */
interface JobTriggerService {
    suspend fun triggerJob(
        jobId: String,
        triggerSource: String = "MANUAL",
        scheduledTimeEpochMs: Long? = null,
        fencingToken: Long = 0L
    ): JobRun
}

class DefaultJobTriggerService(
    private val jobMetadataStore: JobMetadataStore,
    private val runHistoryStore: RunHistoryStore,
    private val outboxStore: OutboxStore,
    private val taskQueue: TaskQueue
) : JobTriggerService {
    private val logger = LoggerFactory.getLogger(DefaultJobTriggerService::class.java)

    override suspend fun triggerJob(
        jobId: String,
        triggerSource: String,
        scheduledTimeEpochMs: Long?,
        fencingToken: Long
    ): JobRun {
        val job = jobMetadataStore.getJob(jobId) ?: throw IllegalArgumentException("Job '$jobId' not found")
        val runId = UUID.randomUUID().toString()
        val now = System.currentTimeMillis()

        val scheduledAt = scheduledTimeEpochMs ?: when (val s = job.schedule) {
            is ScheduleSpec.OneOff -> s.timestampEpochMs
            else -> now
        }

        logger.info("Triggering job '$jobId' (RunId: $runId, Source: $triggerSource, ScheduledAt: $scheduledAt, FencingToken: $fencingToken)")

        val run = JobRun(
            runId = runId,
            jobId = jobId,
            status = JobStatus.RUNNING,
            triggeredAtEpochMs = now,
            startedAtEpochMs = if (scheduledAt <= now) now else null,
            triggerSource = triggerSource
        )
        runHistoryStore.saveRun(run)

        val execution = JobExecution(
            executionId = runId,
            jobId = jobId,
            runId = runId,
            status = JobStatus.QUEUED,
            attempt = 1,
            maxRetries = job.maxRetries,
            action = job.action,
            scheduledAtEpochMs = scheduledAt,
            fencingToken = fencingToken,
            triggerSource = triggerSource
        )
        val outboxEvent = OutboxEvent(
            eventId = UUID.randomUUID().toString(),
            aggregateId = execution.executionId,
            jobExecution = execution
        )
        runHistoryStore.saveTaskInstance(execution)
        outboxStore.saveOutboxEvent(outboxEvent)
        taskQueue.enqueue(execution)
        outboxStore.markOutboxDispatched(outboxEvent.eventId)

        return run
    }
}
