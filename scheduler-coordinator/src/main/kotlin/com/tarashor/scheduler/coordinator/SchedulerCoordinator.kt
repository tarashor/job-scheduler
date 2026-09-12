package com.tarashor.scheduler.coordinator

import com.tarashor.scheduler.cluster.LeaderElector
import com.tarashor.scheduler.cluster.LeaseStore
import com.tarashor.scheduler.core.cron.CronParser
import com.tarashor.scheduler.core.model.*
import com.tarashor.scheduler.queue.TaskQueue
import com.tarashor.scheduler.storage.CompositeSchedulerStorage
import com.tarashor.scheduler.storage.JobMetadataStore
import com.tarashor.scheduler.storage.RunHistoryStore
import com.tarashor.scheduler.storage.SchedulerStorage
import com.tarashor.scheduler.storage.WorkerRegistry
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

class SchedulerCoordinator(
    val coordinatorId: String,
    val leaseStore: LeaseStore,
    val jobMetadataStore: JobMetadataStore,
    val runHistoryStore: RunHistoryStore,
    val workerRegistry: WorkerRegistry,
    val taskQueue: TaskQueue,
    private val tickIntervalMs: Long = 1_000,
    private val workerHeartbeatTimeoutMs: Long = 8_000,
    private val leaseDurationMs: Long = 6_000
) {
    val storage: SchedulerStorage = CompositeSchedulerStorage(jobMetadataStore, runHistoryStore, workerRegistry)

    // Backward-compatible constructor for composite/monolithic storage
    constructor(
        coordinatorId: String,
        leaseStore: LeaseStore,
        storage: SchedulerStorage,
        taskQueue: TaskQueue,
        tickIntervalMs: Long = 1_000,
        workerHeartbeatTimeoutMs: Long = 8_000,
        leaseDurationMs: Long = 6_000
    ) : this(
        coordinatorId = coordinatorId,
        leaseStore = leaseStore,
        jobMetadataStore = storage,
        runHistoryStore = storage,
        workerRegistry = storage,
        taskQueue = taskQueue,
        tickIntervalMs = tickIntervalMs,
        workerHeartbeatTimeoutMs = workerHeartbeatTimeoutMs,
        leaseDurationMs = leaseDurationMs
    )
    private val logger = LoggerFactory.getLogger("Coordinator-$coordinatorId")
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val isRunning = AtomicBoolean(false)

    // Cron tracking: jobId -> nextScheduledRunEpochMs
    private val nextCronRuns = ConcurrentHashMap<String, Long>()

    val outboxDispatcher = com.tarashor.scheduler.outbox.TransactionalOutboxDispatcher(storage, taskQueue)

    val leaderElector = LeaderElector(
        nodeId = coordinatorId,
        leaseStore = leaseStore,
        leaseDurationMs = leaseDurationMs,
        heartbeatIntervalMs = 2_000,
        onElected = { lease ->
            logger.info("Coordinator '$coordinatorId' became LEADER with fencing token ${lease.fencingToken}")
            onBecameLeader(lease.fencingToken)
        },
        onRevoked = {
            logger.warn("Coordinator '$coordinatorId' LOST leadership! Pausing scheduler loops.")
        }
    )

    fun isLeader(): Boolean = leaderElector.isLeader()
    fun getActiveFencingToken(): Long = leaderElector.getActiveLease()?.fencingToken ?: 0L

    fun start() {
        if (!isRunning.compareAndSet(false, true)) return
        logger.info("Starting SchedulerCoordinator '$coordinatorId'")
        leaderElector.start()
        outboxDispatcher.start(pollIntervalMs = 500L)

        // 1. Scheduling clock loop (runs only when this node is leader)
        scope.launch {
            while (isRunning.get()) {
                if (isLeader()) {
                    try {
                        tickSchedulingClock()
                        reapDeadWorkers()
                    } catch (e: CancellationException) {
                        break
                    } catch (e: Exception) {
                        logger.error("Error during coordinator tick", e)
                    }
                }
                delay(tickIntervalMs)
            }
        }
    }

    private fun onBecameLeader(fencingToken: Long) {
        // Initialize next execution times for all cron/interval jobs
        val jobs = storage.listJobs().filter { it.enabled }
        val now = System.currentTimeMillis()
        for (job in jobs) {
            when (val sched = job.schedule) {
                is ScheduleSpec.Cron -> {
                    val parser = CronParser(sched.cronExpression)
                    nextCronRuns[job.jobId] = parser.nextExecution(now)
                }
                is ScheduleSpec.Interval -> {
                    nextCronRuns[job.jobId] = now + sched.intervalMs
                }
                else -> {}
            }
        }
    }

    private suspend fun tickSchedulingClock() {
        val now = System.currentTimeMillis()
        val jobs = storage.listJobs().filter { it.enabled }

        for (job in jobs) {
            when (val sched = job.schedule) {
                is ScheduleSpec.Immediate -> {
                    triggerJob(job.jobId, triggerSource = "IMMEDIATE")
                    // Disable immediate job after triggering once
                    storage.saveJob(job.copy(enabled = false))
                }
                is ScheduleSpec.OneOff -> {
                    if (now >= sched.timestampEpochMs) {
                        triggerJob(job.jobId, triggerSource = "ONE_OFF")
                        // Disable one-off job after trigger
                        storage.saveJob(job.copy(enabled = false))
                    }
                }
                is ScheduleSpec.Cron -> {
                    val nextRun = nextCronRuns.computeIfAbsent(job.jobId) {
                        CronParser(sched.cronExpression).nextExecution(now)
                    }
                    if (now >= nextRun) {
                        triggerJob(job.jobId, triggerSource = "CRON")
                        val updatedNext = CronParser(sched.cronExpression).nextExecution(now)
                        nextCronRuns[job.jobId] = updatedNext
                    }
                }
                is ScheduleSpec.Interval -> {
                    val nextRun = nextCronRuns.computeIfAbsent(job.jobId) { now + sched.intervalMs }
                    if (now >= nextRun) {
                        triggerJob(job.jobId, triggerSource = "INTERVAL")
                        nextCronRuns[job.jobId] = now + sched.intervalMs
                    }
                }
            }
        }
    }

    suspend fun triggerJob(jobId: String, triggerSource: String = "MANUAL"): JobRun {
        val job = storage.getJob(jobId) ?: throw IllegalArgumentException("Job '$jobId' not found")
        val runId = UUID.randomUUID().toString()
        val now = System.currentTimeMillis()
        val fencingToken = getActiveFencingToken()

        logger.info("Triggering job '$jobId' (RunId: $runId, Source: $triggerSource, FencingToken: $fencingToken)")

        val run = JobRun(
            runId = runId,
            jobId = jobId,
            status = JobStatus.RUNNING,
            triggeredAtEpochMs = now,
            startedAtEpochMs = now,
            triggerSource = triggerSource
        )
        storage.saveRun(run)

        // Transactional Outbox + Fast Path Enqueue
        for (taskSpec in job.tasks) {
            val instance = TaskInstance(
                taskInstanceId = "$runId-${taskSpec.taskId}-1",
                runId = runId,
                jobId = jobId,
                taskId = taskSpec.taskId,
                status = TaskStatus.QUEUED,
                attempt = 1,
                maxRetries = taskSpec.maxRetries,
                action = taskSpec.action,
                scheduledAtEpochMs = now,
                fencingToken = fencingToken
            )
            val outboxEvent = OutboxEvent(
                eventId = UUID.randomUUID().toString(),
                aggregateId = instance.taskInstanceId,
                taskInstance = instance
            )
            storage.saveTaskInstance(instance)
            storage.saveOutboxEvent(outboxEvent)
            taskQueue.enqueue(instance)
            storage.markOutboxDispatched(outboxEvent.eventId)
        }

        return run
    }

    suspend fun handleTaskCompletion(taskInstance: TaskInstance, result: TaskExecutionResult) {
        val job = storage.getJob(taskInstance.jobId) ?: return
        val run = storage.getRun(taskInstance.runId) ?: return
        if (run.status != JobStatus.RUNNING) return

        if (result.success) {
            val completedInst = taskInstance.copy(
                status = TaskStatus.COMPLETED,
                completedAtEpochMs = System.currentTimeMillis(),
                output = result.output
            )
            storage.saveTaskInstance(completedInst)
            logger.info("Task '${taskInstance.taskId}' in run '${run.runId}' COMPLETED successfully")

            // Check if all tasks in this run are completed
            val allInstances = storage.getTaskInstancesForRun(taskInstance.runId)
            if (allInstances.all { it.status == TaskStatus.COMPLETED }) {
                storage.saveRun(
                    run.copy(
                        status = JobStatus.COMPLETED,
                        completedAtEpochMs = System.currentTimeMillis()
                    )
                )
                logger.info("JobRun '${run.runId}' finished with status: COMPLETED")
            }
        } else {
            // Task failed
            logger.warn("Task '${taskInstance.taskId}' failed: ${result.error}")
            val nextAttempt = taskInstance.attempt + 1
            if (nextAttempt <= taskInstance.maxRetries) {
                // Retry with backoff
                val retried = taskQueue.requeueWithBackoff(taskInstance, result.error ?: "Task execution failed")
                storage.saveTaskInstance(retried)
            } else {
                // Max retries exhausted -> Move to DLQ & Fail JobRun
                val dlqEntry = taskQueue.sendToDlq(taskInstance, "Exhausted ${taskInstance.maxRetries} retries: ${result.error}")
                val dlqInst = taskInstance.copy(
                    status = TaskStatus.DEAD_LETTER,
                    completedAtEpochMs = System.currentTimeMillis(),
                    error = dlqEntry.reason
                )
                storage.saveTaskInstance(dlqInst)
                storage.saveRun(
                    run.copy(
                        status = JobStatus.FAILED,
                        completedAtEpochMs = System.currentTimeMillis(),
                        error = "Task '${taskInstance.taskId}' permanently failed: ${result.error}"
                    )
                )
                logger.error("JobRun '${run.runId}' marked as FAILED due to task '${taskInstance.taskId}'")
            }
        }
    }

    private suspend fun reapDeadWorkers() {
        val now = System.currentTimeMillis()
        val workers = storage.listWorkers()

        for (worker in workers) {
            if (worker.status == WorkerStatus.HEALTHY && (now - worker.lastHeartbeatEpochMs) > workerHeartbeatTimeoutMs) {
                logger.error("Worker '${worker.workerId}' missed heartbeats for ${now - worker.lastHeartbeatEpochMs}ms! Marking DEAD.")
                val deadWorker = worker.copy(status = WorkerStatus.DEAD, currentLoad = 0)
                storage.upsertWorker(deadWorker)

                // Reclaim all tasks assigned to this dead worker
                val activeTasks = storage.findActiveTaskInstances().filter { it.assignedWorkerId == worker.workerId }
                for (task in activeTasks) {
                    logger.warn("Reclaiming task '${task.taskInstanceId}' from dead worker '${worker.workerId}'")
                    val requeued = taskQueue.requeueWithBackoff(task, "Worker '${worker.workerId}' crashed / timed out")
                    storage.saveTaskInstance(requeued)
                }
            }
        }
    }

    suspend fun stop() {
        if (isRunning.compareAndSet(true, false)) {
            logger.info("Stopping Coordinator '$coordinatorId'")
            leaderElector.stop()
            outboxDispatcher.stop()
            scope.cancel()
        }
    }
}
