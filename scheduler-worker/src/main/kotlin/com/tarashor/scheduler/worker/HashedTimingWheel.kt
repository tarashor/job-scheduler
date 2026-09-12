package com.tarashor.scheduler.worker

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * Production-grade Hashed Timing Wheel (George Varghese & Anthony Lauck algorithm).
 * Provides O(1) timer scheduling and execution with sub-50ms micro-precision.
 *
 * Used by WorkerNode to hold pre-fetched tasks in memory until the exact target millisecond,
 * completely eliminating network roundtrips, database queries, and Redis contention at execution time.
 */
class HashedTimingWheel(
    val tickDurationMs: Long = 20L,
    val wheelSize: Int = 512,
    private val scope: CoroutineScope = CoroutineScope(Dispatchers.Default + SupervisorJob())
) {
    init {
        require(wheelSize > 0 && (wheelSize and (wheelSize - 1)) == 0) {
            "wheelSize must be a power of 2, but was $wheelSize"
        }
    }

    private val logger = LoggerFactory.getLogger(HashedTimingWheel::class.java)
    private val mask = wheelSize - 1

    private data class TimerTaskEntry(
        val taskId: String,
        val targetEpochMs: Long,
        var remainingRounds: Long,
        val action: suspend () -> Unit
    )

    private val buckets: Array<ConcurrentLinkedQueue<TimerTaskEntry>> = Array(wheelSize) {
        ConcurrentLinkedQueue<TimerTaskEntry>()
    }

    private val currentTick = AtomicLong(0)
    private val isRunning = AtomicBoolean(false)
    private var tickerJob: Job? = null

    fun start() {
        if (!isRunning.compareAndSet(false, true)) return
        logger.info("HashedTimingWheel started (tickDuration=${tickDurationMs}ms, wheelSize=$wheelSize)")
        tickerJob = scope.launch {
            while (isRunning.get()) {
                val tickStart = System.currentTimeMillis()
                try {
                    tick()
                } catch (e: CancellationException) {
                    break
                } catch (e: Exception) {
                    logger.error("Error during TimingWheel tick", e)
                }
                val elapsed = System.currentTimeMillis() - tickStart
                val sleepTime = maxOf(1L, tickDurationMs - elapsed)
                delay(sleepTime)
            }
        }
    }

    /**
     * Schedules a task to execute at exact targetEpochMs.
     * If the targetEpochMs is already reached or in the past, runs immediately.
     */
    fun schedule(taskId: String, targetEpochMs: Long, action: suspend () -> Unit) {
        val now = System.currentTimeMillis()
        val delayMs = targetEpochMs - now

        if (delayMs <= 0) {
            scope.launch {
                try {
                    action()
                } catch (e: Exception) {
                    logger.error("Error executing immediate task '$taskId'", e)
                }
            }
            return
        }

        val ticks = delayMs / tickDurationMs
        val rounds = ticks / wheelSize
        val tickPos = (currentTick.get() + ticks).toInt() and mask

        val entry = TimerTaskEntry(
            taskId = taskId,
            targetEpochMs = targetEpochMs,
            remainingRounds = rounds,
            action = action
        )

        buckets[tickPos].add(entry)
    }

    private fun tick() {
        val tick = currentTick.getAndIncrement()
        val bucketIdx = tick.toInt() and mask
        val bucket = buckets[bucketIdx]

        val iterator = bucket.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next()
            if (entry.remainingRounds <= 0) {
                iterator.remove()
                scope.launch {
                    try {
                        entry.action()
                    } catch (e: Exception) {
                        logger.error("Error executing task '${entry.taskId}' from TimingWheel", e)
                    }
                }
            } else {
                entry.remainingRounds--
            }
        }
    }

    fun stop() {
        if (isRunning.compareAndSet(true, false)) {
            logger.info("Stopping HashedTimingWheel")
            tickerJob?.cancel()
        }
    }
}
