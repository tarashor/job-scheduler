package com.tarashor.scheduler.worker

import com.tarashor.scheduler.core.model.*
import com.tarashor.scheduler.queue.TaskQueue
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
    private val heartbeatIntervalMs: Long = 2_000,
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
        onTaskCompleted: (suspend (TaskInstance, TaskExecutionResult) -> Unit)? = null
    ) : this(
        workerId = workerId,
        capacity = capacity,
        taskQueue = taskQueue,
        workerRegistry = storage,
        runHistoryStore = storage,
        taskRunner = taskRunner,
        heartbeatIntervalMs = heartbeatIntervalMs,
        onTaskCompleted = onTaskCompleted
    )
    private val logger = LoggerFactory.getLogger("Worker-$workerId")
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val isRunning = AtomicBoolean(false)
    private val currentLoad = AtomicInteger(0)
    private val activeTasks = ConcurrentHashMap<String, TaskInstance>()

    fun start() {
        if (!isRunning.compareAndSet(false, true)) return
        logger.info("Worker '$workerId' starting with capacity $capacity")

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
                    delay(100)
                    continue
                }

                val task = taskQueue.poll(maxWaitMs = 1000)
                if (task != null) {
                    processTask(task)
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
        val runningTask = task.copy(
            status = TaskStatus.RUNNING,
            assignedWorkerId = workerId,
            startedAtEpochMs = System.currentTimeMillis(),
            lastHeartbeatEpochMs = System.currentTimeMillis()
        )
        activeTasks[runningTask.taskInstanceId] = runningTask
        runHistoryStore?.saveTaskInstance(runningTask)
        logger.info("Worker '$workerId' picked up task '${runningTask.taskInstanceId}' (Attempt ${runningTask.attempt})")

        try {
            val timeoutMs = 30_000L // Default timeout, can be customized per task
            val result = taskRunner.execute(runningTask, timeoutMs)

            activeTasks.remove(runningTask.taskInstanceId)
            currentLoad.decrementAndGet()

            val completedTask = if (result.success) {
                runningTask.copy(
                    status = TaskStatus.COMPLETED,
                    completedAtEpochMs = System.currentTimeMillis(),
                    output = result.output,
                    error = null
                )
            } else {
                runningTask.copy(
                    status = TaskStatus.FAILED,
                    completedAtEpochMs = System.currentTimeMillis(),
                    error = result.error
                )
            }
            runHistoryStore?.saveTaskInstance(completedTask)

            if (onTaskCompleted != null) {
                onTaskCompleted.invoke(completedTask, result)
            }
        } catch (e: Exception) {
            activeTasks.remove(runningTask.taskInstanceId)
            currentLoad.decrementAndGet()
            logger.error("Unexpected exception processing task '${runningTask.taskInstanceId}'", e)
            val failedTask = runningTask.copy(
                status = TaskStatus.FAILED,
                completedAtEpochMs = System.currentTimeMillis(),
                error = e.message
            )
            runHistoryStore?.saveTaskInstance(failedTask)
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
            scope.cancel()
        }
    }
}
