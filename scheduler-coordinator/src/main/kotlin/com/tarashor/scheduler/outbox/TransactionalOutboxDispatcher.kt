package com.tarashor.scheduler.outbox

import com.tarashor.scheduler.queue.TaskQueue
import com.tarashor.scheduler.storage.OutboxStore
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Production-grade Transactional Outbox Dispatcher.
 * Guarantees zero-loss at-least-once task dispatching by draining pending OutboxEvents
 * from durable storage into the Sharded TaskQueue.
 */
class TransactionalOutboxDispatcher(
    private val outboxStore: OutboxStore,
    private val taskQueue: TaskQueue,
    private val scope: CoroutineScope = CoroutineScope(Dispatchers.Default + SupervisorJob())
) {
    private val logger = LoggerFactory.getLogger(TransactionalOutboxDispatcher::class.java)
    private val isRunning = AtomicBoolean(false)
    private var workerJob: Job? = null

    fun start(pollIntervalMs: Long = 500L) {
        if (!isRunning.compareAndSet(false, true)) return
        logger.info("Starting TransactionalOutboxDispatcher (pollInterval=${pollIntervalMs}ms)")
        workerJob = scope.launch {
            while (isRunning.get()) {
                try {
                    dispatchPendingBatch()
                } catch (e: CancellationException) {
                    break
                } catch (e: Exception) {
                    logger.error("Error in TransactionalOutboxDispatcher loop", e)
                }
                delay(pollIntervalMs)
            }
        }
    }

    /**
     * Drains pending outbox events and pushes them to the TaskQueue.
     * Returns the number of events dispatched.
     */
    suspend fun dispatchPendingBatch(limit: Int = 100): Int {
        val pendingEvents = outboxStore.fetchPendingOutboxEvents(limit)
        if (pendingEvents.isEmpty()) return 0

        var dispatchedCount = 0
        for (event in pendingEvents) {
            try {
                taskQueue.enqueue(event.taskInstance)
                outboxStore.markOutboxDispatched(event.eventId, System.currentTimeMillis())
                dispatchedCount++
                logger.info("Outbox: Dispatched task '${event.taskInstance.taskInstanceId}' to TaskQueue")
            } catch (e: Exception) {
                logger.error("Outbox: Failed to dispatch task instance '${event.taskInstance.taskInstanceId}'", e)
            }
        }
        return dispatchedCount
    }

    fun stop() {
        if (isRunning.compareAndSet(true, false)) {
            logger.info("Stopping TransactionalOutboxDispatcher")
            workerJob?.cancel()
        }
    }
}
