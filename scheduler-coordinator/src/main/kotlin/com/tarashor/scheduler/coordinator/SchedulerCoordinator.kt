package com.tarashor.scheduler.coordinator

import com.tarashor.scheduler.cluster.LeaderElector
import com.tarashor.scheduler.cluster.LeaseStore
import com.tarashor.scheduler.coordinator.service.*
import com.tarashor.scheduler.core.model.JobRun
import com.tarashor.scheduler.core.model.TaskExecutionResult
import com.tarashor.scheduler.core.model.TaskInstance
import com.tarashor.scheduler.outbox.TransactionalOutboxDispatcher
import com.tarashor.scheduler.queue.TaskQueue
import com.tarashor.scheduler.service.DefaultJobTriggerService
import com.tarashor.scheduler.service.JobTriggerService
import com.tarashor.scheduler.storage.*
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Clean Architecture Scheduler Coordinator.
 * Composes single-responsibility domain services:
 * - [JobSchedulingService]: Stateless active-active schedule evaluation & dispatch.
 * - [JobTriggerService]: Atomic job execution and outbox enqueueing.
 * - [WorkerReconciliationService]: Dead worker detection and task reclamation.
 * - [JobExecutionService]: Execution state transitions and DLQ routing.
 *
 * Microservices Architecture:
 * When [statelessActiveActive] is true (default), coordinators run as 100% stateless peers
 * without requiring single-leader election or failover downtime.
 */
class SchedulerCoordinator(
    val coordinatorId: String,
    val leaseStore: LeaseStore,
    val jobMetadataStore: JobMetadataStore,
    val runHistoryStore: RunHistoryStore,
    val workerRegistry: WorkerRegistry,
    val taskQueue: TaskQueue,
    val outboxStore: OutboxStore = InMemoryOutboxStore(),
    val triggerService: JobTriggerService = DefaultJobTriggerService(jobMetadataStore, runHistoryStore, outboxStore, taskQueue),
    private val tickIntervalMs: Long = 1_000,
    private val workerHeartbeatTimeoutMs: Long = 8_000,
    private val leaseDurationMs: Long = 6_000,
    val statelessActiveActive: Boolean = true
) {
    val storage: SchedulerStorage = CompositeSchedulerStorage(jobMetadataStore, runHistoryStore, workerRegistry, outboxStore)

    // Backward-compatible constructor for monolithic storage
    constructor(
        coordinatorId: String,
        leaseStore: LeaseStore,
        storage: SchedulerStorage,
        taskQueue: TaskQueue,
        tickIntervalMs: Long = 1_000,
        workerHeartbeatTimeoutMs: Long = 8_000,
        leaseDurationMs: Long = 6_000,
        statelessActiveActive: Boolean = true
    ) : this(
        coordinatorId = coordinatorId,
        leaseStore = leaseStore,
        jobMetadataStore = storage,
        runHistoryStore = storage,
        workerRegistry = storage,
        taskQueue = taskQueue,
        outboxStore = storage,
        tickIntervalMs = tickIntervalMs,
        workerHeartbeatTimeoutMs = workerHeartbeatTimeoutMs,
        leaseDurationMs = leaseDurationMs,
        statelessActiveActive = statelessActiveActive
    )

    private val logger = LoggerFactory.getLogger("Coordinator-$coordinatorId")
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val isRunning = AtomicBoolean(false)

    // Composed Domain Services (SOLID SRP)
    val schedulingService: JobSchedulingService = DefaultJobSchedulingService(jobMetadataStore, triggerService, leaseStore)
    val reconciliationService: WorkerReconciliationService = DefaultWorkerReconciliationService(workerRegistry, runHistoryStore, taskQueue)
    val executionService: JobExecutionService = DefaultJobExecutionService(runHistoryStore, taskQueue)

    val outboxDispatcher = TransactionalOutboxDispatcher(storage, taskQueue)

    val leaderElector = LeaderElector(
        nodeId = coordinatorId,
        leaseStore = leaseStore,
        leaseDurationMs = leaseDurationMs,
        heartbeatIntervalMs = 2_000,
        onElected = { lease ->
            logger.info("Coordinator '$coordinatorId' elected LEADER (FencingToken=${lease.fencingToken})")
        },
        onRevoked = {
            logger.warn("Coordinator '$coordinatorId' revoked from leadership")
        }
    )

    fun isLeader(): Boolean = if (statelessActiveActive) true else leaderElector.isLeader()
    fun getActiveFencingToken(): Long = leaderElector.getActiveLease()?.fencingToken ?: 0L

    fun start() {
        if (!isRunning.compareAndSet(false, true)) return
        logger.info("Starting SchedulerCoordinator '$coordinatorId' (statelessActiveActive=$statelessActiveActive)")
        leaderElector.start()
        outboxDispatcher.start(pollIntervalMs = 500L)

        // Scheduling loop: in stateless active-active mode, all peer pods execute cooperatively.
        scope.launch {
            while (isRunning.get()) {
                if (isLeader()) {
                    try {
                        val token = getActiveFencingToken()
                        schedulingService.tick(token)
                        reconciliationService.reapDeadWorkers(workerHeartbeatTimeoutMs)
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

    suspend fun triggerJob(
        jobId: String,
        triggerSource: String = "MANUAL",
        scheduledTimeEpochMs: Long? = null
    ): JobRun {
        return triggerService.triggerJob(
            jobId = jobId,
            triggerSource = triggerSource,
            scheduledTimeEpochMs = scheduledTimeEpochMs,
            fencingToken = getActiveFencingToken()
        )
    }

    suspend fun handleTaskCompletion(taskInstance: TaskInstance, result: TaskExecutionResult) {
        executionService.handleExecutionCompletion(taskInstance, result)
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
