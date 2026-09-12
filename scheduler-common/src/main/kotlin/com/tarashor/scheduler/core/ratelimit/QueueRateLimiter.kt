package com.tarashor.scheduler.core.ratelimit

import com.tarashor.scheduler.core.model.QueueSpec
import com.tarashor.scheduler.core.model.QueueState
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * Thread-safe Token Bucket rate limiter implementing Google Cloud Tasks
 * maxDispatchesPerSecond and maxBurstSize throttling.
 */
class TokenBucket(
    var ratePerSecond: Double,
    var capacity: Int
) {
    private var tokens: Double = capacity.toDouble()
    private var lastRefillTimestamp: Long = System.currentTimeMillis()
    private val lock = Any()

    fun tryAcquire(tokensRequested: Double = 1.0): Boolean = synchronized(lock) {
        refill()
        if (tokens >= tokensRequested) {
            tokens -= tokensRequested
            true
        } else {
            false
        }
    }

    private fun refill() {
        val now = System.currentTimeMillis()
        val elapsedMs = now - lastRefillTimestamp
        if (elapsedMs > 0) {
            val replenished = (elapsedMs * ratePerSecond) / 1000.0
            tokens = (tokens + replenished).coerceAtMost(capacity.toDouble())
            lastRefillTimestamp = now
        }
    }

    fun availableTokens(): Double = synchronized(lock) {
        refill()
        tokens
    }
}

/**
 * Registry managing per-queue rate limits (Token Bucket) and concurrency limits
 * (maxConcurrentDispatches) according to Google Cloud Tasks specification.
 */
class QueueRateLimiterRegistry {
    private val buckets = ConcurrentHashMap<String, TokenBucket>()
    private val activeCounters = ConcurrentHashMap<String, AtomicInteger>()

    fun canDispatch(queue: QueueSpec): Boolean {
        if (queue.state != QueueState.RUNNING) return false

        val active = activeCounters.computeIfAbsent(queue.queueId) { AtomicInteger(0) }.get()
        if (active >= queue.rateLimits.maxConcurrentDispatches) {
            return false
        }

        val bucket = buckets.compute(queue.queueId) { _, existing ->
            if (existing == null || existing.ratePerSecond != queue.rateLimits.maxDispatchesPerSecond || existing.capacity != queue.rateLimits.maxBurstSize) {
                TokenBucket(queue.rateLimits.maxDispatchesPerSecond, queue.rateLimits.maxBurstSize)
            } else {
                existing
            }
        }!!

        return bucket.tryAcquire(1.0)
    }

    fun onTaskDispatched(queueId: String) {
        activeCounters.computeIfAbsent(queueId) { AtomicInteger(0) }.incrementAndGet()
    }

    fun onTaskFinished(queueId: String) {
        activeCounters.computeIfAbsent(queueId) { AtomicInteger(0) }.updateAndGet { current ->
            (current - 1).coerceAtLeast(0)
        }
    }

    fun getActiveCount(queueId: String): Int =
        activeCounters[queueId]?.get()?.coerceAtLeast(0) ?: 0

    fun reset() {
        buckets.clear()
        activeCounters.clear()
    }
}
