package com.tarashor.scheduler.coordinator

import com.tarashor.scheduler.cluster.LeaderElector
import com.tarashor.scheduler.cluster.LeaseStore
import com.tarashor.scheduler.core.cron.CronParser
import com.tarashor.scheduler.core.dag.DAGEngine
import com.tarashor.scheduler.core.model.*
import com.tarashor.scheduler.queue.TaskQueue
import com.tarashor.scheduler.storage.SchedulerStorage
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

class SchedulerCoordinator(
    val coordinatorId: String,
    private val leaseStore: LeaseStore,
    val storage: SchedulerStorage,
    val taskQueue: TaskQueue,
    private val tickIntervalMs: Long = 1_000,
    private val workerHeartbeatTimeoutMs: Long = 8_000,
    private val leaseDurationMs: Long = 6_000
) {
    private val logger = LoggerFactory.getLogger("Coordinator-$coordinatorId")
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val isRunning = AtomicBoolean(false)

    // Cron tracking: jobId -> nextScheduledRunEpochMs
    private val nextCronRuns = ConcurrentHashMap<String, Long>()

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

        // 1. Scheduling & DAG ticker loop (runs only when this node is leader)
        scope.launch {
            while (isRunning.get()) {
                if (isLeader()) {
                    try {
                        tickSchedulingClock()
                        tickActiveDagRuns()
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

    private suspend fun tickActiveDagRuns() {
        val activeRuns = storage.listRuns(50).filter { it.status == JobStatus.RUNNING }
        for (run in activeRuns) {
            val job = storage.getJob(run.jobId) ?: continue
            val instances = storage.getTaskInstancesForRun(run.runId).associateBy { it.taskId }.toMutableMap()

            // 1. Check if newly ready tasks exist
            val readyTasks = DAGEngine.findReadyTasks(job.tasks, instances)
            for (readyTask in readyTasks) {
                val currentInst = instances[readyTask.taskId] ?: TaskInstance(
                    taskInstanceId = "${run.runId}-${readyTask.taskId}-1",
                    runId = run.runId,
                    jobId = job.jobId,
                    taskId = readyTask.taskId,
                    status = TaskStatus.READY,
                    attempt = 1,
                    maxRetries = readyTask.maxRetries,
                    action = readyTask.action,
                    scheduledAtEpochMs = System.currentTimeMillis(),
                    fencingToken = getActiveFencingToken()
                )
                val queued = currentInst.copy(status = TaskStatus.QUEUED)
                storage.saveTaskInstance(queued)
                instances[readyTask.taskId] = queued
                taskQueue.enqueue(queued)
                logger.info("DAG tick: Enqueued ready task '${readyTask.taskId}' for run '${run.runId}'")
            }

            // 2. Check overall status
            val status = DAGEngine.evaluateJobStatus(job.tasks, instances)
            if (status != JobStatus.RUNNING) {
                storage.saveRun(
                    run.copy(
                        status = status,
                        completedAtEpochMs = System.currentTimeMillis()
                    )
                )
                logger.info("DAG tick: Run '${run.runId}' evaluated to terminal status: $status")
            }
        }
    }

    suspend fun triggerJob(jobId: String, triggerSource: String = "MANUAL"): JobRun {
        val job = storage.getJob(jobId) ?: throw IllegalArgumentException("Job '$jobId' not found")
        val runId = UUID.randomUUID().toString()
        val now = System.currentTimeMillis()
        val fencingToken = getActiveFencingToken()

        logger.info("Triggering job '$jobId' (RunId: $runId, Source: $triggerSource, FencingToken: $fencingToken)")

        // Validate DAG structure
        DAGEngine.validateAndSort(job.tasks)

        val run = JobRun(
            runId = runId,
            jobId = jobId,
            status = JobStatus.RUNNING,
            triggeredAtEpochMs = now,
            startedAtEpochMs = now,
            triggerSource = triggerSource
        )
        storage.saveRun(run)

        // Create TaskInstances for each TaskSpec
        val initialInstances = mutableMapOf<String, TaskInstance>()
        for (taskSpec in job.tasks) {
            val instance = TaskInstance(
                taskInstanceId = "$runId-${taskSpec.taskId}-1",
                runId = runId,
                jobId = jobId,
                taskId = taskSpec.taskId,
                status = TaskStatus.WAITING_DEPENDENCIES,
                attempt = 1,
                maxRetries = taskSpec.maxRetries,
                action = taskSpec.action,
                scheduledAtEpochMs = now,
                fencingToken = fencingToken
            )
            storage.saveTaskInstance(instance)
            initialInstances[taskSpec.taskId] = instance
        }

        // Find initial ready tasks (in-degree = 0) and enqueue them
        val readyTasks = DAGEngine.findReadyTasks(job.tasks, initialInstances)
        for (task in readyTasks) {
            val inst = initialInstances[task.taskId]!!
            val queuedInst = inst.copy(status = TaskStatus.QUEUED)
            storage.saveTaskInstance(queuedInst)
            taskQueue.enqueue(queuedInst)
        }

        return run
    }

    suspend fun handleTaskCompletion(taskInstance: TaskInstance, result: TaskExecutionResult) {
        val job = storage.getJob(taskInstance.jobId) ?: return
        val run = storage.getRun(taskInstance.runId) ?: return
        if (run.status != JobStatus.RUNNING) return

        val instances = storage.getTaskInstancesForRun(taskInstance.runId).associateBy { it.taskId }.toMutableMap()

        if (result.success) {
            val completedInst = taskInstance.copy(
                status = TaskStatus.COMPLETED,
                completedAtEpochMs = System.currentTimeMillis(),
                output = result.output
            )
            storage.saveTaskInstance(completedInst)
            instances[taskInstance.taskId] = completedInst
            logger.info("Task '${taskInstance.taskId}' in run '${run.runId}' COMPLETED successfully")

            // Evaluate downstream tasks in DAG
            val readyTasks = DAGEngine.findReadyTasks(job.tasks, instances)
            for (readyTask in readyTasks) {
                val currentInst = instances[readyTask.taskId] ?: TaskInstance(
                    taskInstanceId = "${run.runId}-${readyTask.taskId}-1",
                    runId = run.runId,
                    jobId = job.jobId,
                    taskId = readyTask.taskId,
                    status = TaskStatus.READY,
                    attempt = 1,
                    maxRetries = readyTask.maxRetries,
                    action = readyTask.action,
                    scheduledAtEpochMs = System.currentTimeMillis(),
                    fencingToken = getActiveFencingToken()
                )
                val queued = currentInst.copy(status = TaskStatus.QUEUED)
                storage.saveTaskInstance(queued)
                instances[readyTask.taskId] = queued
                taskQueue.enqueue(queued)
                logger.info("DAG: Dependent task '${readyTask.taskId}' is now READY and QUEUED")
            }

            // Check if entire JobRun is complete
            val overallStatus = DAGEngine.evaluateJobStatus(job.tasks, instances)
            if (overallStatus == JobStatus.COMPLETED) {
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
            scope.cancel()
        }
    }
}
