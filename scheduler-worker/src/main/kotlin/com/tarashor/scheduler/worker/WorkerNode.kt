package com.tarashor.scheduler.worker

import com.tarashor.scheduler.core.model.*
import com.tarashor.scheduler.queue.TaskQueue
import com.tarashor.scheduler.service.DefaultExecutionCompletionService
import com.tarashor.scheduler.service.ExecutionCompletionService
import com.tarashor.scheduler.storage.RunHistoryStore
import com.tarashor.scheduler.storage.SchedulerStorage
import com.tarashor.scheduler.storage.WorkerRegistry
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

class WorkerNode(
    val workerId: String,
    val capacity: Int = 4,
    private val taskQueue: TaskQueue,
    private val workerRegistry: WorkerRegistry,
    private val runHistoryStore: RunHistoryStore? = null,
    private val taskRunner: TaskRunner = DefaultTaskRunner(),
    private val completionService: ExecutionCompletionService? = null,
    private val heartbeatIntervalMs: Long = 2_000,
    val prefetchLookaheadMs: Long = 5_000,
    val timingWheel: HashedTimingWheel = HashedTimingWheel(tickDurationMs = 20L),
    private val onTaskCompleted: (suspend (TaskInstance, TaskExecutionResult) -> Unit)? = null
) {
    // Backward-compatible constructor for monolithic/composite storage
    constructor(
        workerId: String,
        capacity: Int = 4,
        taskQueue: TaskQueue,
        storage: SchedulerStorage,
        taskRunner: TaskRunner = DefaultTaskRunner(),
        heartbeatIntervalMs: Long = 2_000,
        prefetchLookaheadMs: Long = 5_000,
        onTaskCompleted: (suspend (TaskInstance, TaskExecutionResult) -> Unit)? = null
    ) : this(
        workerId = workerId,
        capacity = capacity,
        taskQueue = taskQueue,
        workerRegistry = storage,
        runHistoryStore = storage,
        taskRunner = taskRunner,
        heartbeatIntervalMs = heartbeatIntervalMs,
        prefetchLookaheadMs = prefetchLookaheadMs,
        timingWheel = HashedTimingWheel(tickDurationMs = 20L),
        onTaskCompleted = onTaskCompleted
    )
    private val logger = LoggerFactory.getLogger("Worker-$workerId")
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val isRunning = AtomicBoolean(false)
    private val currentLoad = AtomicInteger(0)
    private val activeTasks = ConcurrentHashMap<String, TaskInstance>()
    private val effectiveCompletionService: ExecutionCompletionService? =
        runHistoryStore?.let {
            // A supplied callback is the legacy remote/coordinator completion
            // path. Do not also apply the local transition or failures would be
            // retried twice. In standalone workers, use the shared local service.
            completionService ?: if (onTaskCompleted == null) {
                DefaultExecutionCompletionService(it, taskQueue)
            } else {
                null
            }
        }

    fun start() {
        if (!isRunning.compareAndSet(false, true)) return
        logger.info("Worker '$workerId' starting with capacity $capacity and HashedTimingWheel")
        timingWheel.start()

        // 1. Heartbeat loop
        scope.launch {
            while (isRunning.get()) {
                try {
                    sendHeartbeat()
                } catch (e: CancellationException) {
                    break
                } catch (e: Exception) {
                    logger.error("Heartbeat error in worker '$workerId'", e)
                }
                delay(heartbeatIntervalMs)
            }
        }

        // 2. Worker execution loops up to capacity
        for (slot in 1..capacity) {
            scope.launch {
                workerLoop(slot)
            }
        }
    }

    private fun sendHeartbeat() {
        val info = WorkerInfo(
            workerId = workerId,
            capacity = capacity,
            currentLoad = currentLoad.get(),
            lastHeartbeatEpochMs = System.currentTimeMillis(),
            status = WorkerStatus.HEALTHY,
            activeTaskIds = activeTasks.keys.toSet()
        )
        workerRegistry.upsertWorker(info)
    }

    private suspend fun workerLoop(slot: Int) {
        while (isRunning.get()) {
            try {
                if (currentLoad.get() >= capacity) {
                    delay(50)
                    continue
                }

                val task = taskQueue.poll(lookaheadMs = prefetchLookaheadMs, maxWaitMs = 1000)
                if (task != null) {
                    val delayMs = task.scheduledAtEpochMs - System.currentTimeMillis()
                    if (delayMs <= 0L) {
                        processTask(task)
                    } else {
                        // Pre-reserve capacity slot and schedule into HashedTimingWheel
                        currentLoad.incrementAndGet()
                        timingWheel.schedule(task.taskInstanceId, task.scheduledAtEpochMs) {
                            executeTaskCore(task)
                        }
                    }
                }
            } catch (e: CancellationException) {
                break
            } catch (e: Exception) {
                logger.error("Worker slot $slot encountered error", e)
                delay(500)
            }
        }
    }

    private suspend fun processTask(task: TaskInstance) {
        currentLoad.incrementAndGet()
        executeTaskCore(task)
    }

    private suspend fun executeTaskCore(task: TaskInstance) {
        val runningTask = task.copy(
            status = TaskStatus.RUNNING,
            assignedWorkerId = workerId,
            startedAtEpochMs = System.currentTimeMillis(),
            lastHeartbeatEpochMs = System.currentTimeMillis()
        )
        activeTasks[runningTask.taskInstanceId] = runningTask
        runHistoryStore?.saveTaskInstance(runningTask)
        logger.info("Worker '$workerId' executing task '${runningTask.taskInstanceId}' (Attempt ${runningTask.attempt})")

        val result = try {
            val timeoutMs = 30_000L // Default timeout, can be customized per task
            taskRunner.execute(runningTask, timeoutMs)
        } catch (e: Exception) {
            logger.error("Unexpected exception processing task '${runningTask.taskInstanceId}'", e)
            TaskExecutionResult(
                taskInstanceId = runningTask.taskInstanceId,
                success = false,
                error = e.message ?: e.javaClass.simpleName
            )
        }

        activeTasks.remove(runningTask.taskInstanceId)
        currentLoad.decrementAndGet()

        // State transitions and retry policy have one owner. In a standalone
        // worker this is the shared local service; when a coordinator callback
        // is supplied, that callback is the owner instead.
        if (effectiveCompletionService != null) {
            effectiveCompletionService.complete(runningTask, result)
        } else {
            onTaskCompleted?.invoke(runningTask, result)
        }
    }

    fun stop() {
        if (isRunning.compareAndSet(true, false)) {
            logger.info("Stopping worker '$workerId'")
            val deadInfo = WorkerInfo(
                workerId = workerId,
                capacity = capacity,
                currentLoad = 0,
                lastHeartbeatEpochMs = System.currentTimeMillis(),
                status = WorkerStatus.DEAD,
                activeTaskIds = emptySet()
            )
            workerRegistry.upsertWorker(deadInfo)
            timingWheel.stop()
            scope.cancel()
        }
    }
}
