package com.tarashor.scheduler.coordinator.service

import com.tarashor.scheduler.cluster.DistributedLockStore
import com.tarashor.scheduler.core.cron.CronParser
import com.tarashor.scheduler.core.model.ScheduleSpec
import com.tarashor.scheduler.service.JobTriggerService
import com.tarashor.scheduler.storage.JobMetadataStore
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

/**
 * Single Responsibility: Evaluates time-based job schedules (Immediate, OneOff, Cron, Interval)
 * and dispatches due jobs.
 * Supports stateless active-active peer coordination via distributed idempotency locks.
 */
interface JobSchedulingService {
    suspend fun tick(fencingToken: Long = 0L)
}

class DefaultJobSchedulingService(
    private val jobMetadataStore: JobMetadataStore,
    private val triggerService: JobTriggerService,
    private val lockStore: DistributedLockStore
) : JobSchedulingService {
    private val logger = LoggerFactory.getLogger(DefaultJobSchedulingService::class.java)
    private val nextCronRuns = ConcurrentHashMap<String, Long>()

    override suspend fun tick(fencingToken: Long) {
        val now = System.currentTimeMillis()
        val jobs = jobMetadataStore.listJobs().filter { it.enabled }

        for (job in jobs) {
            when (val sched = job.schedule) {
                is ScheduleSpec.Immediate -> {
                    val lockKey = "lock:immediate:${job.jobId}"
                    if (lockStore.tryAcquireLock(lockKey, durationMs = 86_400_000L)) {
                        logger.info("Stateless Dispatch: Triggering Immediate job '${job.jobId}'")
                        triggerService.triggerJob(job.jobId, triggerSource = "IMMEDIATE", fencingToken = fencingToken)
                        jobMetadataStore.saveJob(job.copy(enabled = false))
                    }
                }
                is ScheduleSpec.OneOff -> {
                    val lockKey = "lock:oneoff:${job.jobId}"
                    if (lockStore.tryAcquireLock(lockKey, durationMs = 86_400_000L)) {
                        logger.info("Stateless Dispatch: Enqueuing OneOff job '${job.jobId}' for ${sched.timestampEpochMs}")
                        triggerService.triggerJob(
                            job.jobId,
                            triggerSource = "ONE_OFF",
                            scheduledTimeEpochMs = sched.timestampEpochMs,
                            fencingToken = fencingToken
                        )
                        jobMetadataStore.saveJob(job.copy(enabled = false))
                    }
                }
                is ScheduleSpec.Cron -> {
                    val nextRun = nextCronRuns.computeIfAbsent(job.jobId) {
                        CronParser(sched.cronExpression).nextExecution(now)
                    }
                    if (now >= nextRun) {
                        val slotLockKey = "sched:cron:${job.jobId}:$nextRun"
                        if (lockStore.tryAcquireLock(slotLockKey, durationMs = 86_400_000L)) {
                            logger.info("Stateless Dispatch: Triggering Cron job '${job.jobId}' for slot $nextRun")
                            triggerService.triggerJob(
                                job.jobId,
                                triggerSource = "CRON",
                                scheduledTimeEpochMs = nextRun,
                                fencingToken = fencingToken
                            )
                        }
                        val updatedNext = CronParser(sched.cronExpression).nextExecution(now)
                        nextCronRuns[job.jobId] = updatedNext
                    }
                }
                is ScheduleSpec.Interval -> {
                    val nextRun = nextCronRuns.computeIfAbsent(job.jobId) { now + sched.intervalMs }
                    if (now >= nextRun) {
                        val slotLockKey = "sched:interval:${job.jobId}:$nextRun"
                        if (lockStore.tryAcquireLock(slotLockKey, durationMs = sched.intervalMs * 2)) {
                            logger.info("Stateless Dispatch: Triggering Interval job '${job.jobId}' for slot $nextRun")
                            triggerService.triggerJob(
                                job.jobId,
                                triggerSource = "INTERVAL",
                                scheduledTimeEpochMs = nextRun,
                                fencingToken = fencingToken
                            )
                        }
                        nextCronRuns[job.jobId] = now + sched.intervalMs
                    }
                }
            }
        }
    }
}
